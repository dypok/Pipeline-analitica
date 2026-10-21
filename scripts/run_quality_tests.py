#!/usr/bin/env python3
"""Runner ejecutor para la suite de pruebas de Calidad y Gobernanza (US-08 y US-09).

Responsable: Sebastián Ortiz (Ingeniero de Datos - Calidad & Gobernanza).
Permite ejecutar de forma limpia y legible las 18 pruebas unitarias
sin depender de configuraciones externas o fixtures de AWS/MinIO.

Uso:
    python scripts/run_quality_tests.py
"""

from __future__ import annotations

import io
import sys
import time
from pathlib import Path
from typing import Callable, List, Tuple

# Compatibilidad con encoding de consola Windows
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# Asegurar que la raíz del proyecto esté en el path de Python
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.quality.validator import DataQualityValidator
import tests.test_quality as test_quality_mod
import tests.test_governance as test_gov_mod


def run_suite(
    suite_name: str,
    test_functions: List[Tuple[str, Callable]],
    validator_instance: DataQualityValidator | None = None,
) -> Tuple[int, int]:
    """Ejecuta un conjunto de pruebas y formatea la salida en consola."""
    print(f"\n{'=' * 75}")
    print(f"  EJECUTANDO: {suite_name}")
    print(f"{'=' * 75}")

    passed = 0
    failed = 0

    for name, func in test_functions:
        t0 = time.perf_counter()
        try:
            if validator_instance is not None:
                func(validator_instance)
            else:
                func()
            elapsed_ms = (time.perf_counter() - t0) * 1000
            print(f"  [PASS]  {name:<48} ({elapsed_ms:6.2f} ms)")
            passed += 1
        except Exception as err:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            print(f"  [FAIL]  {name:<48} ({elapsed_ms:6.2f} ms)")
            print(f"          Error: {err}")
            failed += 1

    return passed, failed


def main() -> int:
    """Punto de entrada principal del test runner."""
    print("\n" + "#" * 75)
    print("  PLATAFORMA MEDALLION -- TEST SUITE DE CALIDAD Y GOBERNANZA")
    print("  Responsable: Sebastian Ortiz | Rama: feature/calidad-gobierno")
    print("#" * 75)

    # 1. Preparar suite de Calidad (US-08)
    validator = DataQualityValidator()
    quality_tests = [
        (name, getattr(test_quality_mod, name))
        for name in sorted(dir(test_quality_mod))
        if name.startswith("test_") and callable(getattr(test_quality_mod, name))
    ]

    # 2. Preparar suite de Gobernanza y Linaje (US-09)
    gov_tests = [
        (name, getattr(test_gov_mod, name))
        for name in sorted(dir(test_gov_mod))
        if name.startswith("test_") and callable(getattr(test_gov_mod, name))
    ]

    total_t0 = time.perf_counter()

    # Ejecutar suites
    q_passed, q_failed = run_suite("Capa de Calidad y Cuarentena (US-08)", quality_tests, validator)
    g_passed, g_failed = run_suite("Capa de Gobernanza, Linaje y PII (US-09)", gov_tests)

    total_passed = q_passed + g_passed
    total_failed = q_failed + g_failed
    total_count = total_passed + total_failed
    total_duration = time.perf_counter() - total_t0

    # Resumen final
    print(f"\n{'=' * 75}")
    print(f"  RESUMEN DE RESULTADOS")
    print(f"{'=' * 75}")
    print(f"  Total de pruebas ejecutadas : {total_count}")
    print(f"  Pruebas exitosas (PASS)     : {total_passed}")
    print(f"  Pruebas fallidas (FAIL)     : {total_failed}")
    print(f"  Tiempo total de ejecucion   : {total_duration:.4f} s")

    if total_failed == 0:
        print("\n  >>> ESTADO: 100% DE PRUEBAS EXITOSAS (CERTIFICADO US-08 Y US-09) <<<\n")
        return 0
    else:
        print(f"\n  >>> ALERTA: {total_failed} PRUEBAS FALLIDAS <<<\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
