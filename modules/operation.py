from enum import Enum


class OperationType(Enum):
    READ = "Read"
    WRITE = "Write"
    UPDATE = "Update"
    COMMIT = "Commit"


class Operation:
    def __init__(self, operation_type: OperationType, node):
        """
        Initializes an operation with a type and the node it operates on.
        """

        if not isinstance(operation_type, OperationType):
            raise ValueError(
                f"Invalid operation type. Must be of type OperationType Enum."
            )

        self.operation_type = operation_type
        self.node = node

    def __repr__(self):
        return f"Operation({self.operation_type.value}, {self.node})"


if __name__ == "__main__":
    op = Operation(OperationType.READ, "x")

    print(op)
