from __future__ import annotations

from pathlib import Path
import io
import argparse
from time import perf_counter

from PIL import Image, ImageOps
from concurrent.futures import ProcessPoolExecutor, as_completed
import os

MAX_SIDE = 2990
MAX_MB = 10
ALLOWED_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".tif", ".tiff", ".bmp"}

WORKERS = max(1, (os.cpu_count() or 4) - 0)


def fit_box(size, max_side: int):
    """Вписываем изображение в квадрат (max_side x max_side), сохраняя пропорции."""
    w, h = size
    if w <= max_side and h <= max_side:
        return w, h
    k = min(max_side / w, max_side / h)
    return int(w * k), int(h * k)


def save_with_limit(img: Image.Image, out_path: Path, max_bytes: int):
    """Сохраняем, уменьшая качество до тех пор, пока файл не <= max_bytes."""
    if img.mode in ("RGBA", "P"):
        img = img.convert("RGB")

    for quality in range(95, 40, -5):  # от 95 до 45
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=quality, optimize=True, progressive=True)
        if buf.tell() <= max_bytes:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_bytes(buf.getvalue())
            return
    # если всё равно больше — сохраняем в самом маленьком качестве
    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(out_path, format="JPEG", quality=40, optimize=True, progressive=True)


def _process_one_worker(args_tuple):
    """
    Изолированный воркер-процесс: НЕ печатает, возвращает результат в главный процесс.
    Возвращает кортеж (ok: bool, rel_str: str, msg: str)
    """
    in_path, out_root, in_root = args_tuple
    rel = in_path.relative_to(in_root)
    out_path = (out_root / rel).with_suffix(".jpg")

    try:
        with Image.open(in_path) as im_raw:
            im = ImageOps.exif_transpose(im_raw)  # учёт ориентации
            new_size = fit_box(im.size, MAX_SIDE)
            if new_size != im.size:
                im = im.resize(new_size, Image.LANCZOS)

            save_with_limit(im, out_path, int(MAX_MB * 1024 * 1024))
        return True, str(rel), str(out_path.relative_to(out_root))
    except Exception as e:
        return False, str(rel), str(e)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Папка с фото")
    args = parser.parse_args()

    in_root = Path(args.input).resolve()
    out_root = Path(__file__).resolve().parent / "convert_image"

    files = [p for p in in_root.rglob("*") if p.suffix.lower() in ALLOWED_EXTS]

    if not files:
        print("Нет подходящих файлов для обработки.")
        return

    # Параллельная обработка
    tasks = [(p, out_root, in_root) for p in files]

    # chunksize помогает снизить накладные расходы на IPC при большом количестве файлов
    chunksize = max(1, len(tasks) // (WORKERS * 8) or 1)

    print(f"Найдено файлов: {len(files)}. Воркеров: {WORKERS}. Chunksize: {chunksize}")

    with ProcessPoolExecutor(max_workers=WORKERS) as executor:
        futures = [executor.submit(_process_one_worker, t) for t in tasks]
        for fut in as_completed(futures):
            ok, rel, msg = fut.result()
            if ok:
                print(f"[OK] {rel} → {msg}")
            else:
                print(f"[ERR] {rel}: {msg}")


if __name__ == "__main__":
    time = perf_counter()
    main()
    print("*" * 100, "\n", perf_counter() - time, "\n", "*" * 100)

#  python convert_images.py --input 'E:\TopHotels\all\отели_медиа\The Oberoi Beach Resort, Sahl Hasheesh'
