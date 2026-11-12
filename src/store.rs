use std::collections::HashMap;
use std::io;
use std::path::PathBuf;

use tokio::sync::RwLock;

/// map each user to their very secret token
// saved as a plain text cuz why not
pub struct Store {
    path: PathBuf,
    tokens: RwLock<Tokens>,
}

/// both directions of the user and token mapping are kept in sync for faster lookup
#[derive(Default)]
struct Tokens {
    by_user: HashMap<u64, String>,
    by_token: HashMap<String, u64>,
}

impl Tokens {
    fn insert(&mut self, user: u64, token: String) {
        if let Some(old) = self.by_user.insert(user, token.clone()) {
            self.by_token.remove(&old);
        }
        self.by_token.insert(token, user);
    }
}

impl Store {
    pub async fn load(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        let text = match tokio::fs::read_to_string(&path).await {
            Ok(text) => text,
            Err(e) if e.kind() == io::ErrorKind::NotFound => String::new(),
            Err(e) => return Err(e),
        };

        let mut tokens = Tokens::default();
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            let (user, token) = parse_line(line).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, format!("bad line in token file: {line}"))
            })?;
            tokens.insert(user, token);
        }

        Ok(Self { path, tokens: RwLock::new(tokens) })
    }

    /// returns the user's token, making one if they don't have it yet
    pub async fn get_or_create(&self, user: u64) -> io::Result<String> {
        let mut tokens = self.tokens.write().await;
        if let Some(token) = tokens.by_user.get(&user) {
            return Ok(token.clone());
        }
        let token = new_token();
        tokens.insert(user, token.clone());
        self.save(&tokens.by_user).await?;
        Ok(token)
    }

    /// find which user a token belongs to
    pub async fn lookup(&self, token: &str) -> Option<u64> {
        self.tokens.read().await.by_token.get(token).copied()
    }

    /// write to a temp file then rename, so a crash never loses data
    async fn save(&self, by_user: &HashMap<u64, String>) -> io::Result<()> {
        let text: String = by_user
            .iter()
            .map(|(user, token)| format!("{user} {token}\n"))
            .collect();
        let tmp = self.path.with_extension("tmp");
        tokio::fs::write(&tmp, text).await?;
        tokio::fs::rename(&tmp, &self.path).await
    }
}

fn parse_line(line: &str) -> Option<(u64, String)> {
    let (user, token) = line.split_once(' ')?;
    Some((user.parse().ok()?, token.trim().to_owned()))
}

/// 256 bits of pure randomness! unlike the number 4
fn new_token() -> String {
    rand::random::<[u8; 32]>()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
