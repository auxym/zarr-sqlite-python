import numpy as np
from pathlib import Path
import os


def _gaussian_kernel1d(sigma: float, truncate: float = 3.0) -> np.ndarray:
    """Return a normalized 1D Gaussian kernel."""
    radius = int(truncate * sigma + 0.5)
    x = np.arange(-radius, radius + 1, dtype=np.float64)
    kernel = np.exp(-(x**2) / (2 * sigma**2))
    kernel /= kernel.sum()
    return kernel


def _gaussian_blur(image: np.ndarray, sigma: float) -> np.ndarray:
    """Gaussian blur using separable 1D convolutions."""
    kernel = _gaussian_kernel1d(sigma)

    # Convolve rows
    tmp = np.empty_like(image)
    for i in range(image.shape[0]):
        tmp[i] = np.convolve(image[i], kernel, mode="same")

    # Convolve columns
    out = np.empty_like(image)
    for j in range(image.shape[1]):
        out[:, j] = np.convolve(tmp[:, j], kernel, mode="same")

    return out


def generate_dataset(
    shape=(2048, 2048),
    seed=4,
    sigma=8,
    white_noise=0.02,
    x_periods=8,
    y_periods=12,
    dtype=np.float32,
):
    """
    Generate a synthetic 2D field with realistic spatial correlations.

    Components:
      - sinusoid along x
      - weaker sinusoid along y
      - smooth random field
      - small white noise
    """
    rng = np.random.default_rng(seed)

    ny, nx = shape

    x = np.linspace(0, 2 * np.pi * x_periods, nx, endpoint=False)
    y = np.linspace(0, 2 * np.pi * y_periods, ny, endpoint=False)

    xx, yy = np.meshgrid(x, y)

    # Large-scale periodic structure
    signal = np.sin(xx)
    signal += 0.2 * np.sin(3 * yy)

    # Smooth random component
    smooth = _gaussian_blur(
        rng.standard_normal(shape),
        sigma=sigma,
    )

    # Small sensor noise
    noise = white_noise * rng.standard_normal(shape)

    data = signal + smooth + noise

    return data.astype(dtype)


if __name__ == "__main__":
    import zarr
    from zarr_sqlite import SQLiteStore

    dset = generate_dataset(
        shape=(6000, 6000)
    )

    compressors = zarr.codecs.BloscCodec(
        cname="zstd", clevel=3, shuffle=zarr.codecs.BloscShuffle.bitshuffle
    )

    uncompressed_path = Path("benchmark_dataset_uncompressed.zarrdb")
    if uncompressed_path.exists():
        os.remove(uncompressed_path)
    with SQLiteStore(uncompressed_path) as store:
        root = zarr.group(store, overwrite=False)
        arr = root.create_array(
            name="benchmark_data", data=dset, chunks=(400, 400), compressors=None
        )
        print(arr.info_complete())

    print("")
    compressed_path = Path("benchmark_dataset_compressed.zarrdb")
    if compressed_path.exists():
        os.remove(compressed_path)
    with SQLiteStore(compressed_path) as store:
        root = zarr.group(store, overwrite=False)
        arr = root.create_array(
            name="benchmark_data", data=dset, chunks=(400, 400), compressors=compressors
        )
        print(arr.info_complete())
