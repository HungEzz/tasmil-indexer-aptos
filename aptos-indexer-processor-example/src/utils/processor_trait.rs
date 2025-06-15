use async_trait::async_trait;
use anyhow::Result;

pub trait ProcessorName {
    fn name(&self) -> String;
}

#[async_trait]
pub trait ProcessorTrait {
    async fn process(&mut self) -> Result<()>;
} 