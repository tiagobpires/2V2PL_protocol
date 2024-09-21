from modules.lock import LockType
from modules.lock_manager import LockManager
from modules.operation import OperationType
from modules.transaction import Transaction


def test_lock_manager():
    lock_manager = LockManager()

    # Criar transações
    transaction1 = Transaction(lock_manager)
    transaction2 = Transaction(lock_manager)

    # Solicitar bloqueios
    assert transaction1.create_operation(OperationType.WRITE, "resource1")
    assert not transaction2.create_operation(
        OperationType.READ, "resource1"
    )  # Deveria falhar

    # Liberar bloqueio
    transaction1.commit_transaction()
    assert transaction2.create_operation(
        OperationType.READ, "resource1"
    )  # Deveria ter sucesso agora

    print("Todos os testes passaram.")


if __name__ == "__main__":
    test_lock_manager()
