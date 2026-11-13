mod cal;
mod store;

use std::env;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use poise::serenity_prelude as serenity;
use tracing::{debug, error, info};

use store::Store;

type Error = Box<dyn std::error::Error + Send + Sync>;
type Context<'a> = poise::Context<'a, App, Error>;

/// how often to re-pull everything from discord
const REFRESH_EVERY: Duration = Duration::from_secs(10 * 60);

#[derive(Clone)]
struct App {
    store: Arc<Store>,
    events: cal::Snapshot,
    public_url: String,
}

impl App {
    fn feed_url(&self, token: &str) -> String {
        format!("{}/cal/{token}.ics", self.public_url)
    }
}

#[poise::command(slash_command, subcommands("link"), subcommand_required)]
async fn calendar(_: Context<'_>) -> Result<(), Error> {
    Ok(())
}

/// get link
#[poise::command(slash_command)]
async fn link(ctx: Context<'_>) -> Result<(), Error> {
    debug!("command used");
    let token = ctx.data().store.get_or_create(ctx.author().id.get()).await?;
    send_link(
        ctx,
        &token,
        "Here's your calendar! It includes all the events that you're interested in from all the servers that we share. Have fun!!!",
    )
    .await
}

/// it's only ever shown to the person who asked.
fn private(content: impl Into<String>) -> poise::CreateReply {
    poise::CreateReply::default().content(content).ephemeral(true)
}

async fn send_link(ctx: Context<'_>, token: &str, heading: &str) -> Result<(), Error> {
    let app = ctx.data();
    let msg = format!(
        "{heading}\n\n\
         Or paste this into your calendar app: `{}`\n\n\
         Only events you've marked **Interested** will show up.",
        app.feed_url(token)
    );
    let button = serenity::CreateButton::new_link(format!("{}/subscribe/{token}", app.public_url))
        .label("Subscribe to Calendar!!!");

    ctx.send(private(msg).components(vec![serenity::CreateActionRow::Buttons(vec![button])]))
        .await?;
    Ok(())
}

async fn feed(State(app): State<App>, Path(file): Path<String>) -> Response {
    let token = file.strip_suffix(".ics").unwrap_or(&file);
    let Some(user_id) = app.store.lookup(token).await else {
        return StatusCode::NOT_FOUND.into_response();
    };

    // still on the first refresh, an empty calendar would wipe the user's events
    let Some(events) = cal::collect_events(&app.events, serenity::UserId::new(user_id)).await
    else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };

    (
        [(header::CONTENT_TYPE, "text/calendar; charset=utf-8")],
        cal::render_ics(&events),
    )
        .into_response()
}

/// discord buttons can't use webcal:// :(, so they link here and we do all the work
async fn subscribe(State(app): State<App>, Path(token): Path<String>) -> Response {
    if app.store.lookup(&token).await.is_none() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let url = app.feed_url(&token);
    let rest = url.split_once("://").map_or(url.as_str(), |(_, r)| r);
    (StatusCode::FOUND, [(header::LOCATION, format!("webcal://{rest}"))]).into_response()
}

/// pulls everything from discord into the snapshot, forever
async fn refresh_loop(
    http: Arc<serenity::Http>,
    cache: Arc<serenity::Cache>,
    events: cal::Snapshot,
) {
    // first tick fires immediately
    let mut tick = tokio::time::interval(REFRESH_EVERY);
    loop {
        tick.tick().await;
        let fresh = cal::refresh(&http, &cache).await;
        *events.write().await = Some(Arc::new(fresh));
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let token = env::var("DISCORD_TOKEN").expect("DISCORD_TOKEN must be set");
    let public_url = env::var("PUBLIC_URL")
        .expect("PUBLIC_URL must be set")
        .trim_end_matches('/')
        .to_string();
    let bind_addr = env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8000".into());
    let store_path = env::var("STORE_PATH").unwrap_or_else(|_| "tokens".into());
    let store = Arc::new(Store::load(store_path).await?);

    let framework = poise::Framework::builder()
        .options(poise::FrameworkOptions {
            commands: vec![calendar()],
            ..Default::default()
        })
        .setup(move |ctx, ready, framework| {
            Box::pin(async move {
                poise::builtins::register_globally(ctx, &framework.options().commands).await?;

                let events: cal::Snapshot = Default::default();
                tokio::spawn(refresh_loop(ctx.http.clone(), ctx.cache.clone(), events.clone()));

                let app = App {
                    store,
                    events,
                    public_url,
                };

                let router = Router::new()
                    .route("/cal/:file", get(feed))
                    .route("/subscribe/:token", get(subscribe))
                    .with_state(app.clone());
                let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
                tokio::spawn(async move {
                    if let Err(e) = axum::serve(listener, router).await {
                        error!(error = %e, "HTTP server error");
                    }
                });

                info!("{} is connected, serving feeds on {bind_addr}", ready.user.name);
                Ok(app)
            })
        })
        .build();

    serenity::ClientBuilder::new(token, serenity::GatewayIntents::GUILDS)
        .framework(framework)
        .await?
        .start()
        .await?;
    Ok(())
}
