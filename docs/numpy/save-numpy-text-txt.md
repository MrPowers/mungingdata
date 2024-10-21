---
title: "Writing NumPy Array to Text Files"
date: "2021-12-24"
categories: 
  - "numpy"
---

# Writing NumPy Array to Text Files

This post explains the different ways to save a NumPy array to text files.

After showing the different syntax options the post will teach you some better ways to write NumPy data:

- using binary file formats that are more flexible
- writing multiple files in parallel so the operation is faster

Let's dig in!

## Writing one dimensional array

Let's create a 1D array and write it to the `Documents` directory.

```
import os
import numpy as np

a = np.array([1, 2, 3])

home = os.path.expanduser("~")
np.savetxt(f"{home}/Documents/numpy/cat.txt", a)
```

Here are the contents of the `cat.txt` file:

```
1
2
3
```

Here's how to get the data to write row-wise instead of column-wise:

```
np.savetxt(f"{home}/Documents/numpy/dog.txt", a, newline=" ")
```

Here are the contents of `dog.txt` (removed zeros for brevity):

```
1 2 3
```

You can also convert the 1D array to a 2D array to get the data to write row-wise.

```
np.savetxt(f"{home}/Documents/numpy/dog2.txt", [a])
```

## Writing two dimensional array

2D arrays get written row-wise, as would be expected. Let's create a two dimensional array and write it out to demonstrate.

```
b = np.array([[1, 2], [3, 4]])

np.savetxt(f"{home}/Documents/numpy/racoon.txt", b)
```

Here are the contents of `racoon.txt`.

```
1 2
3 4
```

The default save behavior for 2D arrays is more intuitive than for 1D arrays.

## Writing three dimensional array

Let's create a 3D array and try to write it out to a text file.

```
c = np.array([[[1, 2], [3, 4]]])

np.savetxt(f"{home}/Documents/numpy/fox.txt", c)
```

Here's the error message that's thrown:

```
---------------------------------------------------------------------------
ValueError                                Traceback (most recent call last)
/var/folders/d2/116lnkgd0l7f51xr7msb2jnh0000gn/T/ipykernel_10594/1463971899.py in <module>
----> 1 np.savetxt(f"{home}/Documents/numpy/fox.txt", c)

<__array_function__ internals> in savetxt(*args, **kwargs)

~/opt/miniconda3/envs/standard-coiled/lib/python3.9/site-packages/numpy/lib/npyio.py in savetxt(fname, X, fmt, delimiter, newline, header, footer, comments, encoding)
   1380         # Handle 1-dimensional arrays
   1381         if X.ndim == 0 or X.ndim > 2:
-> 1382             raise ValueError(
   1383                 "Expected 1D or 2D array, got %dD array instead" % X.ndim)
   1384         elif X.ndim == 1:

ValueError: Expected 1D or 2D array, got 3D array instead
```

Text files cannot handle three dimensional data structures and that's why this operation errors out.

## Writing to NumPy file format

The NumPy file format can handle three dimensional arrays without issue.

```
np.save(f"{home}/Documents/numpy/parrot.npy", c)
```

The `parrot.npy` file is binary and isn't human readable. You can easily read in the file to a NumPy array to inspect the contents instead of opening the file itself.

```
np.load(f"{home}/Documents/numpy/parrot.npy")

# array([[[1, 2],
#         [3, 4]]])
```

It's typically best to store data in binary file formats. The files are smaller and human readability isn't usually important.

## Scaling NumPy

NumPy is a great library, but it has limitations.

NumPy can only handle datasets that are smaller than the memory on your machine. Real world datasets are often too big for NumPy to handle.

NumPy analyses can also be slow because they don't process the data in parallel.

It's better to process larger datasets with parallel processing frameworks like Dask. Dask splits up the data into multiple underlying NumPy arrays, each of which can be operated on in parallel by the different cores of a machine or cluster.

## Conclusion

There are a variety of options when writing NumPy arrays to text files.

Text files are not a great file format for binary data. CSV files are slightly better, but face similar limitations, [as described here](https://crunchcrunchhuman.com/2021/12/25/numpy-save-csv-write/).

NumPy binary files or Zarr files are much better. See [this post on why Zarr is the best option](https://coiled.io/blog/save-numpy-dask-array-to-zarr/).

Writing data to a single file is slow and can cause memory errors if the dataset is large. It's best to write data to multiple files, in parallel, when the dataset is large relative to the memory on your machine. For really big datasets, it's best to leverage the power of a cluster when writing files in parallel.
