from __future__ import annotations
"""
llm_client.py
-------------
Abstração do client LLM para o pipeline do 6J Watcher.

Stack:
  - Llama 3B (via NVIDIA API): Tier 2 — parsing estruturado, geração de contexto
  - DeepSeek V4 (via NVIDIA API): Tier 1 — raciocínio quantitativo sobre dados realtime

Features:
  - Circuit breaker: timeout configurável, max retries
  - Rate limiting: budget máximo de chamadas/hora
  - Graceful degradation: retorna None em caso de falha
"""
import asyncio
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)


class NvidiaLLMClient:
    """Client assíncrono para NVIDIA API com circuit breaker e rate limiting."""

    def __init__(
        self,
        api_key: str,
        context_model: str = "meta/llama-3.1-8b-instruct",
        reasoning_model: str = "deepseek-ai/deepseek-v4",
        timeout: float = 5.0,
        max_calls_per_hour: int = 100,
    ):
        if not api_key:
            raise ValueError("NVIDIA_API_KEY is required for LLM integration")

        self.api_key = api_key
        self.context_model = context_model
        self.reasoning_model = reasoning_model
        self.timeout = timeout
        self.max_calls_per_hour = max_calls_per_hour

        self._call_timestamps: list[float] = []
        self._consecutive_failures = 0
        self._circuit_open = False

        # Importação lazy do httpx para não travar se não estiver instalado
        try:
            import httpx
            self._client = httpx.AsyncClient(
                base_url="https://integrate.api.nvidia.com/v1",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                timeout=self.timeout,
            )
        except ImportError:
            logger.error("[LLM] httpx não instalado. Execute: pip install httpx")
            raise

    def _check_rate_limit(self) -> bool:
        """Verifica se estamos dentro do budget de chamadas/hora."""
        now = time.time()
        cutoff = now - 3600
        self._call_timestamps = [t for t in self._call_timestamps if t > cutoff]
        if len(self._call_timestamps) >= self.max_calls_per_hour:
            logger.warning(
                f"[LLM] Rate limit atingido: {len(self._call_timestamps)}/{self.max_calls_per_hour} calls/hora"
            )
            return False
        return True

    def _check_circuit_breaker(self) -> bool:
        """Circuit breaker: abre após 3 falhas consecutivas."""
        if self._circuit_open:
            logger.warning("[LLM] Circuit breaker ABERTO — suprimindo chamadas")
            return False
        return True

    def _record_success(self):
        self._consecutive_failures = 0
        self._circuit_open = False
        self._call_timestamps.append(time.time())

    def _record_failure(self):
        self._consecutive_failures += 1
        if self._consecutive_failures >= 3:
            self._circuit_open = True
            logger.error("[LLM] Circuit breaker ABERTO após 3 falhas consecutivas")

    async def _call_model(self, model: str, prompt: str, max_tokens: int = 1024) -> Optional[str]:
        """Chamada genérica à NVIDIA API."""
        if not self._check_rate_limit() or not self._check_circuit_breaker():
            return None

        payload = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens,
            "temperature": 0.3,
        }

        try:
            response = await self._client.post("/chat/completions", json=payload)
            response.raise_for_status()
            data = response.json()
            content = data["choices"][0]["message"]["content"]
            self._record_success()
            logger.info(f"[LLM] {model} respondeu ({len(content)} chars)")
            return content
        except Exception as e:
            self._record_failure()
            logger.error(f"[LLM] Falha em {model}: {e}")
            return None

    async def generate_context(self, data: str) -> Optional[str]:
        """Llama 3B: estrutura dados brutos em contexto legível."""
        prompt = (
            "Organize os seguintes dados de order flow em formato estruturado para análise.\n"
            "Mantenha apenas informações relevantes para trading institucional.\n\n"
            f"DADOS:\n{data}\n\n"
            "OUTPUT: JSON estruturado com os campos mais relevantes."
        )
        return await self._call_model(self.context_model, prompt, max_tokens=512)

    async def reason(self, context: str, question: str) -> Optional[str]:
        """DeepSeek V4: raciocina sobre o contexto para gerar insights."""
        prompt = context
        if question:
            prompt += f"\n\nPERGUNTA: {question}"
        return await self._call_model(self.reasoning_model, prompt, max_tokens=1024)

    def reset_circuit_breaker(self):
        """Reset manual do circuit breaker (ex: após resolver problemas de rede)."""
        self._circuit_open = False
        self._consecutive_failures = 0
        logger.info("[LLM] Circuit breaker resetado manualmente")

    async def close(self):
        """Fecha o client HTTP."""
        await self._client.aclose()
