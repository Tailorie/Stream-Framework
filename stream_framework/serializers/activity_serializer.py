import base64
import pickle
import typing as t

from stream_framework.activity import Activity
from stream_framework.serializers.base import BaseSerializer
from stream_framework.utils import datetime_to_epoch, epoch_to_datetime
from stream_framework.verbs import get_verb_by_id


class ActivitySerializer(BaseSerializer):
    """
    Serializer optimized for taking as little memory as possible to store an
    Activity

    Serialization consists of 5 parts
    - actor_id
    - verb_id
    - object_id
    - target_id
    - extra_context (pickle)

    None values are stored as 0
    """

    def dumps(self, activity: Activity) -> str:
        self.check_type(activity)
        # keep the milliseconds
        activity_time = "%.6f" % datetime_to_epoch(activity.time)
        parts = [
            activity.actor_id,
            activity.verb.id,
            activity.object_id,
            activity.target_id or 0,
        ]
        extra_context = activity.extra_context.copy()
        pickle_string = pickle_dump(extra_context)
        parts += [activity_time, pickle_string]
        serialized_activity = ",".join(map(str, parts))
        return serialized_activity

    def loads(self, serialized_activity: str):
        parts = serialized_activity.split(",", 5)
        # convert these to ids
        actor_id, verb_id, object_id, target_id = map(int, parts[:4])
        activity_datetime = epoch_to_datetime(float(parts[4]))
        pickle_string = parts[5]
        if not target_id:
            target_id = None
        verb = get_verb_by_id(verb_id)
        extra_context = pickle_load(pickle_string)
        activity = self.activity_class(
            actor_id,
            verb,
            object_id,
            target_id,
            time=activity_datetime,
            extra_context=extra_context,
        )

        return activity


def pickle_dump(data: dict[str, t.Any]) -> str:
    """Encode the given data using pickle and base64."""

    if not data:
        return ""

    pickled = pickle.dumps(data, protocol=3)
    return base64.b64encode(pickled).decode()


def pickle_load(dump_str: str) -> dict[str, t.Any]:
    """Decode the given pickled data.

    Normally the source should be a base64-encoded pickle dump.

    For backwards compatibility this function also accepts regular pickle data.
    """

    if not dump_str:
        return {}

    pickled = dump_str.encode("latin1")
    try:
        pickled = base64.b64decode(pickled, validate=True)
    except ValueError:
        pass  # Could be old data, not base64-encoded.

    return pickle.loads(pickled)
