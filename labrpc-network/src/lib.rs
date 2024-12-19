use rand::random;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Debug, Clone)]
pub struct NetworkState {
    close: bool,
    reliable: bool,
}

impl NetworkState {
    pub fn set_close(&mut self, close: bool) {
        self.close = close;
    }

    pub fn set_reliable(&mut self, reliable: bool) {
        self.reliable = reliable;
    }

    pub async fn simulate_network<T>(&self, expect_result: T) -> Option<T> {
        if self.close {
            return None;
        }

        // short delay
        if !self.reliable {
            let ms = random::<u64>() % 27;
            sleep(Duration::from_millis(ms)).await;
        }

        // random drop msg
        if !self.reliable && random::<u64>() % 1000 < 100 {
            return None;
        }

        Some(expect_result)
    }
}

impl Default for NetworkState {
    fn default() -> Self {
        Self {
            close: false,
            reliable: true,
        }
    }
}
