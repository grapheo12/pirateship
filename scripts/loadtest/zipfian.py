import numpy as np

def zipfian_sample(start, end, s=2, size=100):
    k = np.arange(start, end + 1)
    weights = 1/np.power(k,s)
    weights = weights/weights.sum()
    samples = np.random.choice(k, size=size, p=weights)
    return samples.tolist()

if __name__ == "__main__":
    x = (zipfian_sample(1, 100))
    x.sort()
    print(x)


