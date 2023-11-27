# StreamPipe
**StreamPipe** is a simple Python library (a single file!) for running **multi-worker pipelines** on **infinite data streams**. The best part about StreamPipe is that the **order of data is preserved** throughout the pipeline. You can chain order-independent workers (think of image processing functions) and order-dependent workers (think of an object video tracker) on the same StreamPipe!

<p align="center">
  <img src="https://github.com/highvight/streampipe/assets/25797927/95e5a820-eb1a-43f7-ad67-8bd8ff94fb12" width="400" align="center">
</p>



## Installation
```bash
pip3 install streampipe
```

Install from source:
```bash
git clone https://github.com/highvight/streampipe
cd streampipe
pip3 install .
pip3 install .[testing]  # for testing and benchmarks
```

## Usage
Here is a starter. Let's check if you can speed up a reshaping and standardization pipeline for images.

```python
import timeit
import numpy as np
from streampipe import StreamPipe

IMG = np.random.rand(1000, 1000, 3) * 255

def _reshape(img):
    return img[::2, ::2]

def _standardize(img):
    mean = np.mean(img, axis=(0, 1), keepdims=True)
    std = np.std(img, axis=(0, 1), keepdims=True)
    return (img - mean) / std

pipe = StreamPipe()
pipe.add(_reshape, n_workers=4, maxsize=4)
pipe.add(_standardize, n_workers=4, maxsize=4)

def measure_pipe():
    # Starts the workers
    with pipe:
        for _ in range(100):
            pipe.put(IMG)
    # All workers stopped, all items done

def measure_normal():
    for _ in range(100):
        x = _reshape(IMG)
        x = _standardize(x)

print(timeit.timeit(measure_pipe, number=1))  # 1.214164252000046
print(timeit.timeit(measure_normal, number=1))  # 3.083142001007218
```
    
## License

[MIT](https://choosealicense.com/licenses/mit/)
