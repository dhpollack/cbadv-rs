use crate::models::adv::websocket::{
    Channel, Endpoint, EndpointStream, EndpointType, Message, WebSocketEndpoints,
};
use crate::types::CbResult;
use std::future::Future;
use tokio_tungstenite::tungstenite::{Error as WsError, Message as WsMessage};

pub trait WebSocketClientTrait {
    fn connect(&self) -> impl Future<Output = CbResult<WebSocketEndpoints>> + Send;
    fn connect_endpoint(
        &self,
        endpoint_type: &EndpointType,
    ) -> impl Future<Output = CbResult<Endpoint>> + Send;
    fn handle_reconnect(
        &mut self,
        endpoint_type: &EndpointType,
    ) -> impl Future<Output = CbResult<Endpoint>> + Send;
    fn wait_on_reconnect(
        &mut self,
        endpoint_type: &EndpointType,
    ) -> impl Future<Output = CbResult<Endpoint>> + Send;
    fn reconnect<E>(
        &mut self,
        stream: E,
    ) -> impl Future<Output = CbResult<WebSocketEndpoints>> + Send
    where
        E: Into<EndpointStream> + Send;
    fn listen<E, F, Fut>(&mut self, endpoints: E, callback: F) -> impl Future<Output = ()> + Send
    where
        E: Into<EndpointStream> + Send,
        F: FnMut(CbResult<Message>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send;
    /// Fetches messages from the WebSocket stream with a limit on the number of messages to fetch.
    ///
    /// NOTE: Adequate pauses / sleeps between calls should be added to prevent busy-looping.
    ///
    /// # Arguments
    ///
    /// * `stream` - The WebSocket stream to get messages from.
    /// * `limit` - The maximum number of messages to fetch. Use `usize::MAX` to fetch all messages.
    /// * `action` - The action to take on each message.
    ///
    /// # Errors
    ///
    /// Returns a `String` if the user returns an error within the action.
    fn fetch_sync<F>(
        &self,
        stream: &mut EndpointStream,
        limit: usize,
        action: F,
    ) -> Result<(), String>
    where
        F: FnMut(CbResult<Message>) -> Result<(), String>;
    fn fetch_async<F, Fut>(
        &self,
        stream: &mut EndpointStream,
        limit: usize,
        action: F,
    ) -> impl Future<Output = Result<(), String>>
    where
        F: FnMut(CbResult<Message>) -> Fut,
        Fut: Future<Output = Result<(), String>>;
    fn wait_on_bucket(&mut self, endpoint: &EndpointType) -> impl Future<Output = ()> + Send;
    fn process_message(message: Result<WsMessage, WsError>) -> Option<CbResult<Message>>;
    fn update(
        &mut self,
        channel: &Channel,
        product_ids: &[String],
        action: &str,
        endpoint: &EndpointType,
    ) -> impl Future<Output = CbResult<()>> + Send;
    fn subscribe(
        &mut self,
        channel: &Channel,
        product_ids: &[String],
    ) -> impl Future<Output = CbResult<()>> + Send;
    fn unsubscribe(
        &mut self,
        channel: &Channel,
        product_ids: &[String],
    ) -> impl Future<Output = CbResult<()>> + Send;
}
