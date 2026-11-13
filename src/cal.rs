use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Duration, Utc};
use futures::future::join_all;
use icalendar::{Calendar, Component, Event, EventLike, EventStatus, Property};
use serde::Deserialize;
use serenity::all::{
    Cache, GuildId, Http, ScheduledEvent, ScheduledEventId, ScheduledEventStatus, Timestamp,
    UserId,
};
use serenity::http::{LightMethod, Request, Route, UserPagination};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// max users discord will send over
const PAGE_SIZE: u64 = 100;

/// RFC 5545 weekday names, discord uses 0 = monday
const DAYS: [&str; 7] = ["MO", "TU", "WE", "TH", "FR", "SA", "SU"];

const DISCORD_EPOCH_MS: u64 = 1_420_070_400_000;

/// every event from every server and will be None until refresh() is done
pub type Snapshot = Arc<RwLock<Option<Arc<Vec<Found>>>>>;

/// discord's recurrence_rule, serenity doesn't parse for some reason?
#[derive(Debug, Default, Deserialize)]
struct Extras {
    recurrence_rule: Option<RecurrenceRule>,
    #[serde(rename = "guild_scheduled_event_exceptions")]
    exceptions: Option<Vec<EventException>>,
}

#[derive(Debug, Deserialize)]
struct RecurrenceRule {
    start: DateTime<Utc>,
    end: Option<DateTime<Utc>>,
    /// 0 yearly, 1 monthly, 2 weekly, 3 daily
    frequency: u8,
    interval: u32,
    by_weekday: Option<Vec<u8>>,
    by_n_weekday: Option<Vec<NWeekday>>,
    by_month: Option<Vec<u8>>,
    by_month_day: Option<Vec<u8>>,
    by_year_day: Option<Vec<u16>>,
    count: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct NWeekday {
    n: i8,
    day: u8,
}

/// occurrence of a recurring event that was canceled
#[derive(Debug, Deserialize)]
struct EventException {
    event_exception_id: String,
    #[serde(default)]
    is_canceled: bool,
    scheduled_start_time: Option<DateTime<Utc>>,
    scheduled_end_time: Option<DateTime<Utc>>,
}

impl EventException {
    fn original_start(&self) -> Option<DateTime<Utc>> {
        let id: u64 = self.event_exception_id.parse().ok()?;
        DateTime::from_timestamp_millis(((id >> 22) + DISCORD_EPOCH_MS) as i64)
    }
}

/// an event the user is interested in, plus everything needed to render it
pub struct Found {
    event: ScheduledEvent,
    extras: Extras,
    guild_name: String,
    interested: HashSet<UserId>,
}

/// get all upcoming events the user is interested in and sorted by start time
pub async fn refresh(http: &Http, cache: &Cache) -> Vec<Found> {
    let started = Instant::now();
    let guilds = cache.guilds();
    info!(guilds = guilds.len(), "refreshing events");

    let per_guild = guilds.into_iter().map(|guild_id| {
        let guild_name = cache
            .guild(guild_id)
            .map_or_else(|| guild_id.to_string(), |g| g.name.clone());
        guild_events(http, guild_id, guild_name)
    });

    let mut found: Vec<Found> = join_all(per_guild).await.into_iter().flatten().collect();
    found.sort_by_key(|f| f.event.start_time);

    info!(
        events = found.len(),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "refreshed events"
    );
    found
}

pub async fn collect_events(snapshot: &Snapshot, user: UserId) -> Option<Vec<Event>> {
    let all = snapshot.read().await.clone()?;
    let mine: Vec<&Found> = all.iter().filter(|f| f.interested.contains(&user)).collect();

    let recurring = mine.iter().filter(|f| f.extras.recurrence_rule.is_some()).count();
    info!(%user, total = mine.len(), recurring, "collected events");

    Some(mine.into_iter().flat_map(to_ical_events).collect())
}

/// raw fetch so we can grab recurrence_rule, which serenity's struct drops
async fn fetch_events(
    http: &Http,
    guild_id: GuildId,
) -> serenity::Result<Vec<(ScheduledEvent, Extras)>> {
    let req = Request::new(Route::GuildScheduledEvents { guild_id }, LightMethod::Get)
        .params(Some(vec![("with_user_count", "true".to_string())]));
    let raw: Vec<serde_json::Value> = http.fire(req).await?;
    debug!(%guild_id, count = raw.len(), "fetched raw events");

    Ok(raw.into_iter().filter_map(|v| parse_event(guild_id, v)).collect())
}

fn parse_event(guild_id: GuildId, v: serde_json::Value) -> Option<(ScheduledEvent, Extras)> {
    debug!("parsing event");
    let extras = Extras::deserialize(&v).unwrap_or_else(|e| {
        warn!(%guild_id, error = %e, "can't parse recurrence data, treating as one-off");
        Extras::default()
    });
    // serenity ignores unknown fields, so the same value works here
    match ScheduledEvent::deserialize(v) {
        Ok(event) => Some((event, extras)),
        Err(e) => {
            warn!(%guild_id, error = %e, "can't parse event");
            None
        },
    }
}

/// every event in one server
async fn guild_events(http: &Http, guild_id: GuildId, guild_name: String) -> Vec<Found> {
    let events = match fetch_events(http, guild_id).await {
        Ok(events) => events,
        Err(e) => {
            warn!(%guild_id, guild = %guild_name, error = %e, "can't fetch events");
            return Vec::new();
        },
    };
    let total = events.len();

    let lookups = events
        .into_iter()
        // nobody interested means the user isn't so get drop that fucker
        .filter(|(event, _)| event.user_count != Some(0))
        .map(|(event, extras)| async move {
            match interested_users(http, guild_id, event.id).await {
                Ok(users) => Some((event, extras, users)),
                Err(e) => {
                    warn!(%guild_id, event = %event.id, error = %e, "can't get interested users");
                    None
                },
            }
        });

    let found: Vec<Found> = join_all(lookups)
        .await
        .into_iter()
        .flatten()
        .map(|(event, extras, interested)| Found {
            event,
            extras,
            guild_name: guild_name.clone(),
            interested,
        })
        .collect();

    debug!(%guild_id, guild = %guild_name, total, kept = found.len(), "refreshed guild");
    found
}

/// everyone interested in an event and max 100 at a time
async fn interested_users(
    http: &Http,
    guild: GuildId,
    event: ScheduledEventId,
) -> serenity::Result<HashSet<UserId>> {
    let mut users = HashSet::new();
    let mut after = None;
    loop {
        let page = guild
            .scheduled_event_users_optioned(
                http,
                event,
                Some(PAGE_SIZE),
                after.map(UserPagination::After),
                Some(false),
            )
            .await?;

        let len = page.len() as u64;
        after = page.last().map(|u| u.user.id);
        users.extend(page.into_iter().map(|u| u.user.id));

        // if a page is < 100 then its the last page
        if len < PAGE_SIZE {
            debug!(%guild, %event, users = users.len(), "fetched interested users");
            return Ok(users);
        }
    }
}

/// discord recurrence_rule -> RFC 5545 RRULE value
fn to_rrule(r: &RecurrenceRule) -> Option<String> {
    let freq = match r.frequency {
        0 => "YEARLY",
        1 => "MONTHLY",
        2 => "WEEKLY",
        3 => "DAILY",
        other => {
            warn!(frequency = other, "unknown recurrence frequency, skipping RRULE");
            return None;
        },
    };
    let mut parts = vec![format!("FREQ={freq}")];
    if r.interval > 1 {
        parts.push(format!("INTERVAL={}", r.interval));
    }

    // plain weekdays and "nth weekday" both go in BYDAY
    let plain = r.by_weekday.iter().flatten().map(|&d| (None, d));
    let nth = r.by_n_weekday.iter().flatten().map(|nw| (Some(nw.n), nw.day));
    let byday: Vec<String> = plain
        .chain(nth)
        .filter_map(|(n, d)| {
            let Some(day) = DAYS.get(d as usize) else {
                warn!(day = d, "weekday out of range, dropping");
                return None;
            };
            Some(n.map_or_else(|| day.to_string(), |n| format!("{n}{day}")))
        })
        .collect();

    let lists = [
        ("BYDAY", (!byday.is_empty()).then(|| byday.join(","))),
        ("BYMONTH", join(&r.by_month)),
        ("BYMONTHDAY", join(&r.by_month_day)),
        ("BYYEARDAY", join(&r.by_year_day)),
    ];
    parts.extend(lists.into_iter().filter_map(|(key, val)| Some(format!("{key}={}", val?))));

    // RFC 5545 says COUNT and UNTIL can't both be set
    match (r.count, r.end) {
        (Some(count), _) => parts.push(format!("COUNT={count}")),
        (None, Some(end)) => parts.push(format!("UNTIL={}", ical_time(end))),
        (None, None) => {},
    }
    Some(parts.join(";"))
}

fn to_ical_events(f: &Found) -> Vec<Event> {
    let event = &f.event;
    let uid = format!("{}@discord-cal", event.id);
    let url = format!("https://discord.com/events/{}/{}", event.guild_id, event.id);

    let location = event
        .metadata
        .as_ref()
        .and_then(|m| m.location.clone())
        .or_else(|| {
            event.channel_id.map(|c| format!("https://discord.com/channels/{}/{c}", event.guild_id))
        })
        .filter(|l| !l.is_empty());

    let description = match event.description.as_deref() {
        Some(d) if !d.is_empty() => format!("{d}\n\nServer: {}\n{url}", f.guild_name),
        _ => format!("Server: {}\n{url}", f.guild_name),
    };

    let next_start = to_utc(event.start_time);
    // vc events have no end time, why? so assume two hours
    let length = event.end_time.map_or(Duration::hours(2), |e| to_utc(e) - next_start);
    if length <= Duration::zero() {
        warn!(event = %event.id, ?length, "event ends before it starts");
    }

    let base = |start: DateTime<Utc>, end: DateTime<Utc>| {
        let mut ev = Event::new();
        ev.uid(&uid)
            .summary(&event.name)
            .description(&description)
            .starts(start)
            .ends(end)
            .url(&url);
        if let Some(loc) = &location {
            ev.location(loc);
        }
        ev
    };

    let rule = f.extras.recurrence_rule.as_ref().and_then(|r| Some((r.start, to_rrule(r)?)));
    let start = rule.as_ref().map_or(next_start, |(s, _)| *s);

    let mut main = base(start, start + length);
    if matches!(event.status, ScheduledEventStatus::Canceled) {
        main.status(EventStatus::Cancelled);
    }
    let Some((_, rrule)) = rule else {
        return vec![main.done()];
    };
    main.add_property("RRULE", &rrule);

    let mut exdates = Vec::new();
    let mut overrides = Vec::new();
    let exceptions = f.extras.exceptions.iter().flatten();
    for (x, original) in exceptions.filter_map(|x| Some((x, x.original_start()?))) {
        if x.is_canceled {
            exdates.push(ical_time(original));
        } else if x.scheduled_start_time.is_some() || x.scheduled_end_time.is_some() {
            let start = x.scheduled_start_time.unwrap_or(original);
            let end = x.scheduled_end_time.unwrap_or(start + length);
            let mut moved = base(start, end);
            moved.add_property("RECURRENCE-ID", ical_time(original));
            overrides.push(moved.done());
        }
    }
    if !exdates.is_empty() {
        main.add_property("EXDATE", exdates.join(","));
    }

    debug!(
        event = %event.id,
        name = %event.name,
        %rrule,
        canceled = exdates.len(),
        moved = overrides.len(),
        "built recurring ical event"
    );
    std::iter::once(main.done()).chain(overrides).collect()
}

/// RFC 5545!!!
pub fn render_ics(events: &[Event]) -> String {
    let mut cal = Calendar::new();
    cal.name("Discord Events!")
        .append_property(("METHOD", "PUBLISH"))
        // how often clients *should* refresh, many hate me and ignore this
        .append_property(
            Property::new("REFRESH-INTERVAL", "PT12H")
                .add_parameter("VALUE", "DURATION")
                .done(),
        )
        .append_property(("X-PUBLISHED-TTL", "PT12H"));

    for ev in events {
        cal.push(ev.clone());
    }
    let out = cal.to_string();
    info!(events = events.len(), bytes = out.len(), "rendered ics");
    out
}

fn join<T: ToString>(v: &Option<Vec<T>>) -> Option<String> {
    let v = v.as_ref().filter(|v| !v.is_empty())?;
    Some(v.iter().map(T::to_string).collect::<Vec<_>>().join(","))
}

fn ical_time(t: DateTime<Utc>) -> String {
    t.format("%Y%m%dT%H%M%SZ").to_string()
}

fn to_utc(ts: Timestamp) -> DateTime<Utc> {
    DateTime::from_timestamp(ts.unix_timestamp(), 0).unwrap_or_else(|| {
        warn!(ts = ts.unix_timestamp(), "timestamp out of range, using epoch");
        DateTime::default()
    })
}
