# RULES.md — Normas de Seguridad y Operación

## 🔐 Reglas de Seguridad Irrompibles

1. **La clave privada (`HL_AGENT_PRIVATE_KEY`) JAMÁS se imprime, registra en logs ni se expone.**
   - Usar siempre `SecretStr` de Pydantic.
   - `__repr__` y `__str__` sobrescritos para ocultar el valor.
   - Ningún módulo fuera de `execution/signer.py` accede al valor descifrado.

2. **Nunca commitear `.env` al repositorio.** Solo `.env.example` está permitido.

3. **El modo mainnet requiere confirmación explícita.**
   - `ENV="mainnet"` exige `MAINNET_CONFIRMATION="I_ACCEPT_THE_RISK"` en el entorno.
   - Sin ella, el proceso lanza `RuntimeError` antes de conectar al exchange.

## ⚙️ Reglas de Arquitectura

4. **Ninguna orden puede enviarse sin pasar por `risk/validator.py`.**
   - El validador retorna un objeto `ApprovedOrder` firmado.
   - El ejecutor rechaza cualquier llamada que no tenga este objeto.

5. **El `kill_switch` es invocable desde tres fuentes independientes:**
   - Comando `/kill` de Telegram.
   - El validador de riesgo (límite diario o drawdown excedido).
   - Archivo `STOP_BOT.flag` en el directorio raíz del proyecto.

6. **El estado del proceso se escribe en SQLite antes de conectar al WebSocket.**
   - Estado `STARTING` → `RUNNING` → `HALTED`.
   - El watchdog no alerta durante los primeros 60 segundos de estado `STARTING`.

## 📊 Reglas de Trading

7. **Máximo 1 posición abierta en BTC en todo momento.**

8. **Stop Loss se coloca en el exchange dentro de los 2 segundos de un fill.**
   - Si falla tras 2 reintentos, cierre de emergencia a mercado inmediato.

9. **Pérdida diaria máxima: 3% del equity inicial del día.**
   - Pausa automática de 24 horas al tocarse.

10. **Drawdown desde pico de equity máximo: 10%.**
    - Activa kill switch permanente. Requiere reinicio manual con confirmación.

11. **Límite de modificaciones de órdenes: máximo 1 cada 5 segundos.**
    - Hyperliquid penaliza cancelaciones excesivas con bloqueo temporal de IP.

12. **SHADOW_MODE activo por defecto en entorno testnet.**
    - En shadow mode, las señales se registran y el PnL virtual se calcula,
      pero no se envía ninguna orden al exchange.
