# Asterisk Monitor API para Zabbix

API responsável por monitorar os logs do Asterisk, transformar as linhas de log em eventos normalizados em JSON e enviá-los para o Zabbix, além de expor esses eventos via HTTP para fins de inspeção e monitoramento.

O serviço foi pensado para rodar em container Docker (por exemplo com o nome `f_zabbix_connector-api`), escutando na porta configurada via variável de ambiente (internamente padrão `8080`, podendo ser publicada como `8082` no host, por exemplo).

---

## Visão geral

Funcionalidades principais:

- Leitura contínua do arquivo de log do Asterisk via SFTP.
- Extração de eventos relevantes (peer reachable/unreachable, entrada em filas, chamadas atendidas, erros, etc.).
- Normalização dos eventos em um formato JSON uniforme.
- Envio dos eventos para o Zabbix usando o protocolo nativo (Zabbix Sender).
- Persistência de eventos em um arquivo de spool local para tolerância a falhas de envio.
- Exposição de endpoints HTTP para:
  - Verificação de saúde do coletor (`/health`).
  - Consulta aos eventos do Asterisk (`/events/asterisk`).
  - Consulta e registro de eventos de serviços externos (`/events/service`).

Os logs da ferramenta são emitidos com níveis `[NOTICE]`, `[WARNING]` e `[ERROR]`, e os eventos normalizados são emitidos em formato JSON (JSONL) na saída padrão e no arquivo de spool.

---

## Arquitetura resumida

Componentes principais do código:

### SFTPTailer

- Mantém conexão SFTP com o servidor onde está o log do Asterisk.
- Faz leitura incremental do arquivo (tail) e detecta rotação.
- Na inicialização de cada ciclo, faz um replay dos últimos minutos de log (janela configurada por `BACKLOG_MINUTES`, padrão 3 minutos).

### ParserWorker (collector)

- Thread responsável por processar as linhas de log recebidas e gerar eventos normalizados.
- Utiliza expressões regulares para identificar tipos de eventos (peer reachable/unreachable, entrada em fila, resposta de agente, etc.).
- Atualiza um “watermark” de timestamp para evitar reprocessamento de linhas já interpretadas.

### ZabbixEmitter

- Serializa eventos em lotes e envia para o servidor Zabbix configurado (`ZBX_SERVER` / `ZBX_PORT` / `ZBX_HOST`).
- Salva todos os eventos em `queue.jsonl` (spool) e, em caso de envio bem-sucedido, remove do início do arquivo as linhas já transmitidas.
- Usa limites de lote (`ZBX_BATCH_MAX`, `ZBX_BATCH_INTERVAL_MS`) para controlar o volume de envio.

### RateLimiter

- Limita a taxa de emissão de eventos para evitar sobrecarga (`RATE_LIMIT_EVENTS_PER_SEC`).
- Eventos que excedem a taxa geram um contador de “suprimidos” (`asterisk.counter.suppressed[...]`).

### Scheduler / Cooldown

- Controla os ciclos do coletor.
- Cada ciclo lê o log até que não haja dados novos relevantes (após um número de polls vazios).
- Ao atingir o estado “idle”, entra em cooldown (`COOLDOWN_SEC`, padrão 60s) e inicia um novo ciclo.
- Um watchdog monitora o heartbeat do coletor; se ficar “stalled”, força cooldown e reinício.

### API FastAPI / Uvicorn

- Expõe os endpoints REST com autenticação via Bearer Token (`API_KEY_MON`).
- Mantém buffers em memória com os eventos mais recentes para consulta.

---

## Formato dos eventos

Os eventos emitidos pela ferramenta seguem um formato JSON padrão, por exemplo:

```json
{
  "ts": "2025-12-10T16:13:44.799122-03:00",
  "key": "asterisk.collector.source_up",
  "value": 1,
  "tags": {},
  "kpi": null,
  "src": {}
}
```

Significado dos campos:

- **`ts`**  
  Timestamp do evento em ISO 8601, com fuso horário configurado (`TIMEZONE`, padrão `America/Sao_Paulo`).

- **`key`**  
  Chave do evento, que identifica o tipo e o contexto. Exemplos:
  - `asterisk.collector.source_up` – coletor conectado à origem (SFTP/log) e em operação.
  - `asterisk.peer.reachable[1002]` – ramal `1002` tornou-se alcançável.
  - `asterisk.log.severity[ERROR,chan_sip.c]` – linha de log de severidade `ERROR` no módulo `chan_sip.c`.

- **`value`**  
  Valor do evento:
  - Em muitos casos, 0 ou 1 (booleano representado como inteiro) para flags.
  - Em outros casos, métricas numéricas (ex.: latência em milissegundos).
  - Para eventos de serviço (`/events/service`), pode ser string de status.

- **`tags`**  
  Dicionário com metadados do evento. Exemplos:
  - `{"peer": "1002"}` para eventos de peer.
  - `{"queue": "fila_suporte", "agent": "1002"}` para eventos de chamadas.
  - Em eventos de serviço: `{"service": "...", "status": "...", "detail": "..."}`.

- **`kpi`**  
  Indicador de que o evento representa uma métrica (KPI).
  - Para eventos gerados pelo parser, se `value` for numérico e diferente de 0 ou 1, `kpi` pode ser `true`.
  - Caso contrário, tende a vir como `null`.

- **`src`**  
  Informações de origem do log, por exemplo:

  ```json
  "src": {
    "file": "full",
    "offset": 103639,
    "raw_ts": "2025-12-10 16:12:59"
  }
  ```

  - `file`: nome do arquivo de log monitorado.
  - `offset`: posição no arquivo em bytes daquela linha.
  - `raw_ts`: timestamp original da linha no formato do Asterisk.

- **`raw`** (opcional)  
  Linha de log completa do Asterisk, quando disponível, por exemplo:

  ```text
  "[2025-12-10 16:12:59] NOTICE[2139165] chan_sip.c: Peer '1002' is now Reachable. (54ms / 2000ms)"
  ```

### Exemplo interpretado

Para o evento:

```json
{
  "ts": "2025-12-10T16:12:59-03:00",
  "key": "asterisk.peer.reachable[1002]",
  "value": 1,
  "tags": {"peer": "1002"},
  "kpi": null,
  "src": {
    "file": "full",
    "offset": 103639,
    "raw_ts": "2025-12-10 16:12:59"
  },
  "raw": "[2025-12-10 16:12:59] NOTICE[2139165] chan_sip.c: Peer '1002' is now Reachable. (54ms / 2000ms)"
}
```

- Indica que o peer/ramal `1002` ficou alcançável (`reachable`) no Asterisk.
- `value = 1` representa “verdadeiro” para esse estado.
- `tags.peer` identifica qual peer foi afetado.
- `src` aponta para a posição exata no log.

---

## Ciclo de coleta e janela de busca

A cada ciclo, o coletor:

1. Conecta ao servidor SFTP e emite o evento `asterisk.collector.source_up` com `value = 1`.
2. Faz um “warm replay” de uma janela de log recente (últimos `BACKLOG_MINUTES`, padrão 3 minutos), garantindo que eventos recentes não sejam perdidos.
3. Entra em loop de leitura incremental, chamando `tail_forward_new_lines()`:
   - Quando há novas linhas, elas são parseadas e convertidas em eventos.
   - Quando não há novos bytes ou não há eventos relevantes, incrementa um contador de polls vazios.
4. Se o número de polls vazios atinge `IDLE_EMPTY_POLLS`, o coletor considera o log “sem atividade nova” e:
   - Marca o status como `idle`.
   - Encerra o ciclo e aciona o cooldown.

### Cooldown

- O `CooldownWorker` aguarda o sinal e inicia um cooldown de `COOLDOWN_SEC` segundos (padrão 60s), exibindo uma barra/contador no log.
- Ao finalizar o cooldown, o scheduler dispara um novo ciclo de coleta.

### Watchdog

- Monitora o tempo desde o último heartbeat.
- Se o coletor estiver em status `running` e ficar sem atualizar por mais de `WATCHDOG_STALL_SEC` segundos, marca como `stalled` e faz log de warning/error, forçando o reinício.

---

## Endpoints da API

Todos os endpoints exigem autenticação via header:

```http
Authorization: Bearer <API_KEY_MON>
```

### `GET /health`

Retorna o estado atual do coletor.

Exemplo de resposta:

```json
{
  "ok": true,
  "time": 1733855620,
  "cycle_id": 3,
  "collector_alive": true,
  "collector_status": "running",
  "collector_last_hb_age_sec": 0.42
}
```

Campos principais:

- `ok`: indica que a API está respondendo.
- `time`: epoch atual (segundos).
- `cycle_id`: identificador do ciclo de coleta atual.
- `collector_alive`: se a thread de coleta está viva.
- `collector_status`: `running`, `idle`, `error` ou `stalled`.
- `collector_last_hb_age_sec`: idade, em segundos, do último heartbeat.

---

### `GET /events/asterisk`

Retorna os eventos recentes do Asterisk armazenados em buffer.

Parâmetros:

- `limit` (query, opcional, padrão `50`): quantidade máxima de eventos mais recentes a retornar.

Exemplo:

```http
GET /events/asterisk?limit=100
Authorization: Bearer <API_KEY_MON>
```

Resposta:

```json
{
  "time": 1733855620,
  "count": 42,
  "events": [
    {
      "ts": "2025-12-10T16:12:59-03:00",
      "key": "asterisk.peer.reachable[1002]",
      "value": 1,
      "tags": {"peer": "1002"},
      "kpi": null,
      "src": {...},
      "raw": "..."
    }
  ]
}
```

---

### `GET /events/service`

Retorna os eventos recentes de serviços externos (registrados via `POST /events/service`).

Parâmetros:

- `limit` (query, opcional, padrão `50`): quantidade máxima de eventos mais recentes a retornar.

Exemplo:

```http
GET /events/service?limit=50
Authorization: Bearer <API_KEY_MON>
```

---

### `POST /events/service`

Permite registrar eventos de outros serviços/sistemas na mesma estrutura de eventos usada para o Zabbix.

Body (JSON):

```json
{
  "service": "report_sgg",
  "status": "ok",
  "detail": "Execução concluída sem erros",
  "kpi": true
}
```

Campos:

- `service` (string, obrigatório): nome lógico do serviço monitorado.
- `status` (string, obrigatório): estado atual (ex.: `ok`, `warning`, `error`, `degraded`).
- `detail` (string, opcional): texto adicional descritivo (é truncado internamente para até 200 caracteres).
- `kpi` (boolean, opcional): sinaliza que o evento deve ser tratado como KPI.

A API converte o corpo em um evento no formato:

```json
{
  "ts": "2025-12-10T16:20:00-03:00",
  "key": "service.monitor[report_sgg]",
  "value": "ok",
  "tags": {
    "service": "report_sgg",
    "status": "ok",
    "detail": "Execução concluída sem erros"
  },
  "kpi": true,
  "src": {"type": "service_push"}
}
```

Esse evento é:

1. Registrado no buffer de `service_events` para consulta via `GET /events/service`.
2. Enfileirado para envio ao Zabbix via `ZabbixEmitter`.

---

## Autenticação

A API é protegida por Bearer Token:

- Definir a variável de ambiente `API_KEY_MON` com o token desejado.
- Todos os endpoints exigem o header:

```http
Authorization: Bearer SEU_TOKEN_AQUI
```

Sem esse header, ou com token inválido, a API responde com HTTP `401 Unauthorized`.

---

## Variáveis de ambiente principais

### Conexão SFTP (origem do log do Asterisk)

- `SFTP_HOST` – host/IP do servidor do Asterisk.
- `SFTP_PORT` – porta SFTP.
- `SFTP_USER` – usuário SFTP.
- `SFTP_PASS` – senha SFTP.
- `SFTP_DIR` – diretório onde está o arquivo de log.
- `SFTP_FILE_ACTIVE` – nome do arquivo de log ativo (ex.: `full`).

### Zabbix

- `ZBX_SERVER` – host/IP do servidor Zabbix.
- `ZBX_PORT` – porta do Zabbix trapper (padrão `10051`).
- `ZBX_HOST` – nome do host configurado no Zabbix que receberá as chaves.
- `ZBX_BATCH_MAX` – tamanho máximo do lote de eventos por envio.
- `ZBX_BATCH_INTERVAL_MS` – intervalo máximo, em milissegundos, entre envios de lote.
- `ZBX_FAIL_SPool` – caminho do arquivo de spool (padrão `/var/spool/asterisk-zbx/queue.jsonl`).

### Coletor / parser

- `POLL_INTERVAL_MS` – intervalo entre polls ao arquivo de log (padrão `2000` ms).
- `RATE_LIMIT_EVENTS_PER_SEC` – limite de eventos por segundo (padrão `50`).
- `MAX_RECONNECT_BACKOFF_MS` – backoff máximo para reconexão SFTP.
- `TIMEZONE` – timezone da aplicação (padrão `America/Sao_Paulo`).
- `IGNORE_AMI_AUTH` – se `true`, ignora falhas de autenticação AMI.
- `IGNORE_AMI_AUTH_IPS` – lista de IPs (separados por vírgula) a serem ignorados em falhas AMI.
- `IDLE_EMPTY_POLLS` – número de polls sem novidade até considerar o coletor “idle”.

### API HTTP

- `API_KEY_MON` – token de autenticação para a API (obrigatório).
- `API_PORT` – porta em que a API escuta dentro do container (padrão `8080`).
- `API_HOST` – host de bind (padrão `0.0.0.0`).

---

## Execução com Docker (exemplo)

Exemplo simplificado de `docker-compose.yml`:

```yaml
version: "3.9"

services:
  asterisk-monitor-api:
    image: sua-imagem/asterisk-monitor-api:latest
    container_name: f_zabbix_connector-api
    restart: unless-stopped
    environment:
      SFTP_HOST: "10.0.0.10"
      SFTP_PORT: 22
      SFTP_USER: "asterisk"
      SFTP_PASS: "senha"
      SFTP_DIR: "/var/log/asterisk"
      SFTP_FILE_ACTIVE: "full"

      ZBX_SERVER: "10.0.0.20"
      ZBX_PORT: 10051
      ZBX_HOST: "PBX01"

      API_KEY_MON: "token-super-secreto"
      API_PORT: 8080
      API_HOST: "0.0.0.0"
    ports:
      - "8082:8080"    # Porta externa 8082 apontando para 8080 interna
    volumes:
      - /var/spool/asterisk-zbx:/var/spool/asterisk-zbx
```

Subida do serviço:

```bash
sudo docker compose up -d
```

Após subir o container:

- A API estará acessível em `http://<IP_DA_VM>:8082` (conforme mapeamento).
- Os logs da aplicação aparecerão no `docker logs f_zabbix_connector-api`.
- Os eventos JSON também serão gravados no arquivo de spool em `/var/spool/asterisk-zbx/queue.jsonl`.

---

## Logs da aplicação

Os logs estruturados da ferramenta são impressos no formato:

```text
[NOTICE] collector: cycle 3 start
[NOTICE] collector: cycle 3 warm replay candidate lines: 120
[NOTICE] collector: cycle 3 poll emitted new events
[WARNING] collector: cycle 3 poll no new bytes
[NOTICE] collector: cycle 3 idle threshold hit. requesting cooldown
[NOTICE] cooldown: starting cooldown 60s
...
```

- Níveis:
  - `NOTICE` – operação normal, início de ciclo, cooldown, etc.
  - `WARNING` – situações anômalas porém não críticas (ex.: polls sem novos bytes, filtros).
  - `ERROR` – falhas de conexão, erros de leitura ou envio de eventos.

Além desses, cada evento emitido também é escrito em JSON (uma linha por evento) na saída padrão e no spool, permitindo auditoria e depuração.

---

## Integração com Zabbix

Para cada evento JSON:

- O `ZabbixEmitter` converte em um item com:
  - `host` = `ZBX_HOST`
  - `key` = `key` do evento (por exemplo, `asterisk.peer.reachable[1002]`)
  - `value` = `value` do evento
  - `clock` = timestamp derivado de `ts`

No Zabbix, é possível:

- Criar itens do tipo *Zabbix trapper* para as chaves desejadas (ex.: `asterisk.collector.source_up`, `asterisk.peer.reachable[*]`, `asterisk.kpi.answer_latency_ms[*]`, etc.).
- Montar gráficos e triggers usando `value` e `tags` como base conceitual.

---

## Ajustes e evolução

Em caso de necessidade de ajustes:

- Novos padrões de log do Asterisk podem ser adicionados por meio de `regex` na seção de parsing (`extract_events`).
- É possível alterar janelas, limiares e taxa de eventos via variáveis de ambiente (sem mudança de código).
- Caso seja necessário adaptar o formato ou os nomes das chaves para melhor encaixe no template de Zabbix, isso pode ser feito diretamente nas funções `build_event` / `build_simple_event` ou na nomenclatura das chaves dentro de `extract_events`.
