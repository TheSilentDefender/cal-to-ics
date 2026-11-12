use chrono::{DateTime, Duration, Utc};
use futures::future::join_all;
use icalendar::{Calendar, Component, Event, EventLike, EventStatus, Property};
use serde::Deserialize;
use serenity::all::{
    Cache, GuildId, Http, ScheduledEvent, ScheduledEventId, ScheduledEventStatus, Timestamp,
    UserId,
};
use serenity::http::{LightMethod, Request, Route, UserPagination};
use tracing::{debug, info, trace, warn};

/// max users discord will send over
const PAGE_SIZE: u64 = 100;

/// RFC 5545 weekday names, discord uses 0 = monday
const DAYS: [&str; 7] = ["MO", "TU", "WE", "TH", "FR", "SA", "SU"];

/// discord's recurrence_rule, serenity doesn't parse for some reason?
#[derive(Debug, Clone, Deserialize)]
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

#[derive(Debug, Clone, Deserialize)]
struct NWeekday {
    n: i8,
    day: u8,
}

/// an event the user is interested in, plus everything needed to render it
struct Found {
    event: ScheduledEvent,
    rule: Option<RecurrenceRule>,
    guild_name: String,
}

/// get all upcoming events the user is interested in and sorted by start time
pub async fn collect_events(http: &Http, cache: &Cache, user: UserId) -> Vec<Event> {
    let guilds = cache.guilds();
    info!(%user, guilds = guilds.len(), "collecting events");

    // one future per server (why is it called guild)
    let per_guild = guilds.into_iter().map(|guild_id| {
        let guild_name = cache
            .guild(guild_id)
            .map(|g| g.name.clone())
            .unwrap_or_else(|| {
                debug!(%guild_id, "guild not in cache, using id as name");
                guild_id.to_string()
            });
        interested_in_guild(http, guild_id, guild_name, user)
    });

    let mut found: Vec<Found> = join_all(per_guild).await.into_iter().flatten().collect();

    let recurring = found.iter().filter(|f| f.rule.is_some()).count();
    info!(%user, total = found.len(), recurring, "collected events");

    found.sort_by_key(|f| f.event.start_time);
    found.iter().map(to_ical_event).collect() // push directly to ical event
}

/// raw fetch so we can grab recurrence_rule, which serenity's struct drops
async fn fetch_events(
    http: &Http,
    guild_id: GuildId,
) -> serenity::Result<Vec<(ScheduledEvent, Option<RecurrenceRule>)>> {
    let req = Request::new(Route::GuildScheduledEvents { guild_id }, LightMethod::Get)
        .params(Some(vec![("with_user_count", "true".to_string())]));
    let raw: Vec<serde_json::Value> = http.fire(req).await?;
    debug!(%guild_id, count = raw.len(), "fetched raw events");

    Ok(raw
        .into_iter()
        .filter_map(|v| {
            let rule = v
                .get("recurrence_rule")
                .filter(|r| !r.is_null())
                .and_then(|r| match serde_json::from_value::<RecurrenceRule>(r.clone()) {
                    Ok(rule) => Some(rule),
                    Err(e) => {
                        // event still shows up, just as a one off
                        warn!(%guild_id, error = %e, raw = %r, "can't parse recurrence_rule");
                        None
                    },
                });
            // serenity ignores unknown fields so the same value works here
            match serde_json::from_value::<ScheduledEvent>(v) {
                Ok(event) => {
                    trace!(%guild_id, event = %event.id, name = %event.name,
                        recurring = rule.is_some(), "parsed event");
                    Some((event, rule))
                },
                Err(e) => {
                    warn!(%guild_id, error = %e, "can't parse event");
                    None
                },
            }
        })
        .collect())
}

/// events in one server the user is interested in
async fn interested_in_guild(
    http: &Http,
    guild_id: GuildId,
    guild_name: String,
    user: UserId,
) -> Vec<Found> {
    let events = match fetch_events(http, guild_id).await {
        Ok(events) => events,
        Err(e) => {
            warn!(%guild_id, guild = %guild_name, error = %e, "can't fetch events");
            return Vec::new();
        },
    };

    let total = events.len();
    let checks = events
        .into_iter()
        // nobody interested means the user isn't so get drop that fucker
        .filter(|(event, _)| {
            let keep = event.user_count != Some(0);
            if !keep {
                trace!(%guild_id, event = %event.id, "skipping, nobody interested");
            }
            keep
        })
        .map(|(event, rule)| async move {
            match is_interested(http, guild_id, event.id, user).await {
                Ok(true) => Some((event, rule)),
                Ok(false) => None,
                Err(e) => {
                    warn!(%guild_id, event = %event.id, error = %e,
                        "can't get interested users");
                    None
                },
            }
        });

    let found: Vec<Found> = join_all(checks)
        .await
        .into_iter()
        .flatten()
        .map(|(event, rule)| Found {
            event,
            rule,
            guild_name: guild_name.clone(),
        })
        .collect();

    debug!(%guild_id, guild = %guild_name, total, interested = found.len(), "checked guild");
    found
}

async fn is_interested(
    http: &Http,
    guild: GuildId,
    event: ScheduledEventId,
    user: UserId,
) -> serenity::Result<bool> {
    let mut after = None;
    let mut pages = 0u32;
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
        pages += 1;
        trace!(%event, page = pages, users = page.len(), "fetched interested users page");

        if page.iter().any(|u| u.user.id == user) {
            debug!(%event, %user, pages, "user is interested");
            return Ok(true);
        }
        // if a page is < 100 then its the last page
        if (page.len() as u64) < PAGE_SIZE {
            trace!(%event, %user, pages, "user not interested");
            return Ok(false);
        }
        after = page.last().map(|u| u.user.id);
    }
}

/// comma join a list, None if missing or empty
fn join<T: ToString>(v: &Option<Vec<T>>) -> Option<String> {
    v.as_ref()
        .filter(|v| !v.is_empty())
        .map(|v| v.iter().map(T::to_string).collect::<Vec<_>>().join(","))
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
    let mut byday: Vec<String> = r
        .by_weekday
        .iter()
        .flatten()
        .filter_map(|&d| {
            let day = DAYS.get(d as usize).map(|s| s.to_string());
            if day.is_none() {
                warn!(day = d, "weekday out of range, dropping");
            }
            day
        })
        .collect();
    byday.extend(r.by_n_weekday.iter().flatten().filter_map(|nw| {
        let day = DAYS.get(nw.day as usize).map(|d| format!("{}{d}", nw.n));
        if day.is_none() {
            warn!(day = nw.day, n = nw.n, "nth weekday out of range, dropping");
        }
        day
    }));
    if !byday.is_empty() {
        parts.push(format!("BYDAY={}", byday.join(",")));
    }

    for (key, val) in [
        ("BYMONTH", join(&r.by_month)),
        ("BYMONTHDAY", join(&r.by_month_day)),
        ("BYYEARDAY", join(&r.by_year_day)),
    ] {
        if let Some(val) = val {
            parts.push(format!("{key}={val}"));
        }
    }

    // RFC 5545 says COUNT and UNTIL can't both be set
    if let Some(count) = r.count {
        if r.end.is_some() {
            debug!("rule has both count and end, using count");
        }
        parts.push(format!("COUNT={count}"));
    } else if let Some(end) = r.end {
        parts.push(format!("UNTIL={}", end.format("%Y%m%dT%H%M%SZ")));
    }
    Some(parts.join(";"))
}

fn to_ical_event(f: &Found) -> Event {
    let (event, guild_name) = (&f.event, f.guild_name.as_str());
    let url = format!("https://discord.com/events/{}/{}", event.guild_id, event.id);

    // external events have free form text, vc events have a channel
    let location = event
        .metadata
        .as_ref()
        .and_then(|m| m.location.clone())
        .or_else(|| {
            event
                .channel_id
                .map(|c| format!("https://discord.com/channels/{}/{c}", event.guild_id))
        })
        .filter(|l| !l.is_empty());

    let description = match event.description.as_deref() {
        Some(d) if !d.is_empty() => format!("{d}\n\nServer: {guild_name}\n{url}"),
        _ => format!("Server: {guild_name}\n{url}"),
    };

    let next_start = to_utc(event.start_time);
    // vc events have no end time, why? so assume two hours
    let length = event.end_time.map_or_else(
        || {
            trace!(event = %event.id, "no end time, assuming two hours");
            Duration::hours(2)
        },
        |e| to_utc(e) - next_start,
    );
    if length <= Duration::zero() {
        warn!(event = %event.id, ?length, "event ends before it starts");
    }

    let start = f.rule.as_ref().map_or(next_start, |r| r.start);
    let end = start + length;

    let mut ev = Event::new();
    ev.uid(&format!("{}@discord-cal", event.id))
        .summary(&event.name)
        .description(&description)
        .starts(start)
        .ends(end)
        .url(&url);
    let rrule = f.rule.as_ref().and_then(to_rrule);
    if let Some(rrule) = &rrule {
        ev.add_property("RRULE", rrule);
    }
    if let Some(loc) = &location {
        ev.location(loc);
    }
    let canceled = matches!(event.status, ScheduledEventStatus::Canceled);
    if canceled {
        ev.status(EventStatus::Cancelled);
    }

    debug!(
        event = %event.id,
        name = %event.name,
        guild = %guild_name,
        %start,
        rrule = rrule.as_deref().unwrap_or("-"),
        canceled,
        "built ical event"
    );
    ev.done()
}

/// RFC 5545!!!
pub fn render_ics(events: &[Event]) -> String {
    let mut cal = Calendar::new();
    cal.name("Discord Events!")
        .append_property(("METHOD", "PUBLISH"))
        // how often clients *should* refresh, many hate me and ignore this
        .append_property(
            Property::new("REFRESH-INTERVAL", "PT12H") //12hrs
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

fn to_utc(ts: Timestamp) -> DateTime<Utc> {
    DateTime::from_timestamp(ts.unix_timestamp(), 0).unwrap_or_else(|| {
        warn!(ts = ts.unix_timestamp(), "timestamp out of range, using epoch");
        DateTime::default()
    })
}
