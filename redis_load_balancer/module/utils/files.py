import fitz
import numpy as np
import asyncio
from PIL import Image


async def save_image(pil_img: Image.Image, file_path: str):
    """이미지 저장을 스레드 풀에서 비동기로 실행"""
    await asyncio.to_thread(pil_img.save, file_path)
    return file_path


def save_pdf_to_images(pdf_path: str, save_path: str, dpi: int = 300, suffix: str = "png"):
    """PDF -> 이미지 변환 + 비동기 저장 (호출자는 동기 함수처럼 그냥 사용)"""
    async def _convert():
        zoom = max(1.0, dpi / 72.0)
        mat = fitz.Matrix(zoom, zoom)
        save_paths = []
        tasks = []

        with fitz.open(pdf_path) as doc:
            for page in doc:
                pix = page.get_pixmap(matrix=mat, alpha=False, colorspace=fitz.csRGB)
                pil_img = pix.pil_image().convert("RGB")
                file_path = save_path / f"{page.number}.{suffix}"

                tasks.append(save_image(pil_img, file_path))
                save_paths.append(file_path)

        if tasks:
            await asyncio.gather(*tasks)

        return save_paths

    # 동기 코드에서 호출할 수 있도록 여기서 이벤트 루프를 만들어 실행
    return asyncio.run(_convert())
