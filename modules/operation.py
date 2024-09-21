from enum import Enum


class OperationType(Enum):
    READ = "R"
    WRITE = "W"
    UPDATE = "U"
    COMMIT = "C"


class Operation:
    def __init__(self, operation_type: OperationType, resource: str):
        """
        Initializes an operation with a type and the resource it operates on.
        """

        if not isinstance(operation_type, OperationType):
            raise ValueError(
                f"Invalid operation type. Must be of type OperationType Enum."
            )

        self.operation_type = operation_type
        self.resource = resource

    def __repr__(self):
        return f"Operation({self.operation_type.value}, {self.resource})"


if __name__ == "__main__":
    op = Operation(OperationType.READ, "x")

    print(op)
