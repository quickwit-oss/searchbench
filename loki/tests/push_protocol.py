"""Independent decoder using upstream push.proto field numbers, not Rust output.

https://github.com/grafana/loki/blob/v3.7.9/pkg/push/push.proto
Used only by tests; no protoc binary or Python protobuf dependency in production.
"""
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory


def push_type():
    file = descriptor_pb2.FileDescriptorProto(name="loki_test_push.proto", package="loki_test", syntax="proto3")
    types = descriptor_pb2.FieldDescriptorProto
    def message(name, fields):
        m = file.message_type.add(name=name)
        for name, number, kind, nested, repeated in fields:
            f = m.field.add(name=name, number=number, type=kind,
                            label=types.LABEL_REPEATED if repeated else types.LABEL_OPTIONAL)
            if nested:
                f.type_name = ".loki_test." + nested
    message("Timestamp", [("seconds", 1, types.TYPE_INT64, None, False), ("nanos", 2, types.TYPE_INT32, None, False)])
    message("LabelPair", [("name", 1, types.TYPE_STRING, None, False), ("value", 2, types.TYPE_STRING, None, False)])
    message("Entry", [("timestamp", 1, types.TYPE_MESSAGE, "Timestamp", False), ("line", 2, types.TYPE_STRING, None, False),
                      ("structured_metadata", 3, types.TYPE_MESSAGE, "LabelPair", True)])
    message("Stream", [("labels", 1, types.TYPE_STRING, None, False), ("entries", 2, types.TYPE_MESSAGE, "Entry", True)])
    message("PushRequest", [("streams", 1, types.TYPE_MESSAGE, "Stream", True)])
    pool = descriptor_pool.DescriptorPool()
    pool.Add(file)
    return message_factory.GetMessageClass(pool.FindMessageTypeByName("loki_test.PushRequest"))


PushRequest = push_type()
