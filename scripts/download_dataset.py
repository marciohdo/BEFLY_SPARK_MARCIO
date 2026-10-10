"""Baixa o dataset Hotel Booking Demand do Kaggle e grava em data/raw/ (idempotente).

- O kagglehub mantém cache local: se a versão já foi baixada, não baixa de novo.
- O CSV só é copiado para data/raw/ se ainda não existir ou se o conteúdo for diferente.
  Use --force para baixar novamente e sobrescrever.
"""

import argparse
import filecmp
import shutil
from pathlib import Path

import kagglehub

DATASET = "jessemostipak/hotel-booking-demand"
FILE_NAME = "hotel_bookings.csv"
RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true", help="Força novo download e sobrescreve data/raw/")
    args = parser.parse_args()

    source = Path(kagglehub.dataset_download(DATASET, force_download=args.force)) / FILE_NAME
    target = RAW_DIR / FILE_NAME
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    if target.exists() and not args.force and filecmp.cmp(source, target, shallow=False):
        print(f"{target} já está atualizado; nada a fazer.")
        return

    shutil.copyfile(source, target)
    print(f"{FILE_NAME} copiado para {target}")


if __name__ == "__main__":
    main()
