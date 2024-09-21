# 2V2PL_protocol

### TODO List:

#### **1. Gerenciamento de Transações e Operações**

- [x] Criar uma classe `Transaction` para representar uma transação.
- [x] Definir as operações possíveis para uma transação: leitura (read), escrita (write), e atualização (update).
- [x] Implementar um gerenciador de transações que controla o estado de cada transação (ativa, bloqueada, abortada, ou comprometida).

#### **2. Gerenciamento de Bloqueios com Suporte a Múltipla Granulosidade**

- [ ] Criar uma classe `Lock` que gerencia diferentes tipos de bloqueios: **shared**, **exclusive**, e **update**.
- [ ] Implementar um **Lock Manager** que gerencia os bloqueios com múltipla granulosidade (por exemplo, bloqueios em nível de registro, página, ou tabela).
- [ ] Implementar lógica para promover bloqueios do tipo **shared** para **update** e, em seguida, para **exclusive**.
- [ ] Garantir que o Lock Manager evite conflitos de bloqueio entre transações concorrentes.

#### **3. Protocolo 2V2PL**

- [ ] Implementar o protocolo de bloqueio em duas fases:
  - Fase 1: Aquisição de todos os bloqueios (antes da execução das operações).
  - Fase 2: Liberação de todos os bloqueios (após a conclusão da transação).
- [ ] Garantir que o protocolo siga a abordagem conservadora, evitando situações onde uma transação possa bloquear outra indefinidamente.

#### **4. Detecção e Prevenção de Deadlocks**

- [x] Criar uma classe `Graph` com detecção de ciclos.
- [ ] Implementar a detecção de deadlocks utilizando o **grafo de espera** (wait-for-graph) com a classe `Graph` já implementada.
- [ ] Implementar a lógica de criação do grafo de espera à medida que os bloqueios são solicitados.
- [ ] Integrar a detecção de ciclos no grafo de espera para identificar deadlocks.
- [ ] Implementar uma estratégia de resolução de deadlock, abortando uma das transações envolvidas no ciclo.

#### **5. Sincronização das Operações**

- [ ] Desenvolver uma função para reordenar as operações no escalonamento, respeitando as restrições de bloqueio e as fases do protocolo 2V2PL.
- [ ] Garantir que a sincronização entre as transações respeite os bloqueios adquiridos e liberados corretamente.

#### **6. Entrada e Saída**

- [ ] Definir o formato de entrada para o escalonamento (conjunto de transações e suas operações).
- [ ] Implementar a lógica para processar a entrada, interpretar transações e suas operações.
- [ ] Definir a estrutura de saída que mostre a execução correta do escalonamento e o status das transações (com sucesso ou abortada).
