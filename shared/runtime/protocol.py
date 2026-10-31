"""Load protoc descriptors independently of its Python code generator version."""
from pathlib import Path
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory


class Protocol:
    """Own an isolated message registry shared by one process."""
    def __init__(self, descriptor_path):
        data = Path(descriptor_path).read_bytes()
        if len(data) > 1024 * 1024:
            raise ValueError("descriptor exceeds supported size")
        files = descriptor_pb2.FileDescriptorSet.FromString(data)
        self.pool = descriptor_pool.DescriptorPool()
        for descriptor in files.file:
            self.pool.AddSerializedFile(descriptor.SerializeToString())
        self.Request = self.message("arena.rpc.Request")
        self.Response = self.message("arena.rpc.Response")

    def message(self, name):
        return message_factory.GetMessageClass(self.pool.FindMessageTypeByName(name))
