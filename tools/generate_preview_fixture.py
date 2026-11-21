"""Regenerate only the fictional UI fixture; optional existing Pillow required.

No camera, downloaded images, audio or network input. Runtime needs no Pillow.
Every WebP VP8 keyframe is wrapped in a minimal finite WebM container.
"""
from io import BytesIO
from pathlib import Path
import struct


def element(tag, payload):
    size = len(payload)
    width = next(n for n in range(1, 9) if size < (1 << (7 * n)) - 1)
    return bytes.fromhex(tag) + ((1 << (7 * width)) | size).to_bytes(width, "big") + payload


def generate():
    from PIL import Image, ImageDraw

    header = element("1a45dfa3", element("4286", b"\x01") + element("42f7", b"\x01")
                     + element("42f2", b"\x04") + element("42f3", b"\x08")
                     + element("4282", b"webm") + element("4287", b"\x02")
                     + element("4285", b"\x02"))
    info = element("1549a966", element("2ad7b1", (1000000).to_bytes(3, "big"))
                   + element("4489", struct.pack(">d", 2000))
                   + element("4d80", b"Sentinel synthetic") + element("5741", b"Sentinel synthetic"))
    track = element("1654ae6b", element("ae", element("d7", b"\x01")
                    + element("73c5", b"\x01") + element("83", b"\x01")
                    + element("86", b"V_VP8") + element("23e383", (100000000).to_bytes(4, "big"))
                    + element("e0", element("b0", b"\xa0") + element("ba", b"\x5a"))))
    frames = []
    for frame in range(20):
        image = Image.new("RGB", (160, 90), "#142b25")
        draw = ImageDraw.Draw(image)
        draw.text((8, 8), "SYNTHETIC DEMO", fill="white")
        draw.text((8, 24), "NO CAMERA / NO AUDIO", fill="white")
        x = 8 + frame * 6
        draw.rectangle((x, 53, x + 14, 72), fill="#a8d77f")
        output = BytesIO()
        image.save(output, format="WEBP", quality=60, method=6)
        raw = output.getvalue()
        assert raw[:4] == b"RIFF" and raw[8:12] == b"WEBP"
        offset = 12
        while raw[offset:offset + 4] != b"VP8 ":
            length = int.from_bytes(raw[offset + 4:offset + 8], "little")
            offset += 8 + length + length % 2
            assert offset < len(raw)
        length = int.from_bytes(raw[offset + 4:offset + 8], "little")
        vp8 = raw[offset + 8:offset + 8 + length]
        frames.append(element("a3", b"\x81" + struct.pack(">h", frame * 100) + b"\x80" + vp8))
    cluster = element("1f43b675", element("e7", b"\x00") + b"".join(frames))
    result = header + element("18538067", info + track + cluster)
    assert len(result) < 65536
    return result


if __name__ == "__main__":
    target = Path(__file__).resolve().parents[1] / "sentinel" / "synthetic_preview.webm"
    target.write_bytes(generate())
    print("Generated fictional 160x90 VP8 WebM; no audio or camera input.")
