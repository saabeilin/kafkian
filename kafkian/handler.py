import typing

from kafkian import Consumer
from kafkian.message import Message


class Handler:
    def __init__(self, consumer: Consumer, synchronous_commits: bool = True):
        self._consumer = consumer
        self._synchronous_commits = synchronous_commits
        self._handlers_map: typing.Dict[
            typing.Tuple[str, typing.Optional[str]], typing.Callable
        ] = {}

    def _before(self, message: Message) -> None:
        self._consumer.metrics.send_consumer_message_metrics(message)

    def _after(self, message: Message) -> None:
        self._consumer.commit(self._synchronous_commits)

    def run(self) -> None:
        for message in self._consumer:
            self._before(message)
            self._handle(message)
            self._after(message)

    def _get_handler_callable(self, message: Message) -> typing.Callable:
        # TODO: implement pattern/glob matching

        # Tombstone message?
        if not message.value:
            return (
                self._handlers_map.get((message.topic, None), None)
                or self._handlers_map.get((message.topic, "*"), None)
                or self._handlers_map.get(("*", None), None)
                or self._handlers_map.get(("*", "*"), None)
            )

        # Non-Avro value?
        if not hasattr(message.value, "schema"):
            return self._handlers_map.get(
                (message.topic, "*"), None
            ) or self._handlers_map.get(("*", "*"), None)

        # Finally, Avro-encoded value
        return (
            self._handlers_map.get((message.topic, message.value.schema.fullname), None)
            or self._handlers_map.get((message.topic, "*"), None)
            or self._handlers_map.get(("*", "*"), None)
        )

    def _handle(self, message: Message) -> None:
        handler = self._get_handler_callable(message)
        handler(message)

    def handles(self, topic: str, avro_schema: str = "*", **kwargs):
        """A decorator to mark a callable as handler for (topic, avro_schema)

        @handler.handles(topic='orders.orders', avro_schema='orders.OrderCreated')
        def on_order_created():
            ...
        """

        def decorator(f):
            self._add_handler(f, topic, avro_schema, **kwargs)
            return f

        return decorator

    def _add_handler(
        self,
        handler: typing.Callable,
        topic: str,
        avro_schema: typing.Optional[str],
        **kwargs,
    ) -> None:
        self._handlers_map[(topic, avro_schema)] = handler
