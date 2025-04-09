import random


def mod_inverse(a, p):
    return pow(a, -1, p)

def split_secret(secret, n, t, p):
    coeffs = [secret] + [random.randrange(0, p) for _ in range(t - 1)]
    shares = []
    for i in range(1, n + 1):
        accum = 0
        for power, coeff in enumerate(coeffs):

            accum = (accum + coeff * pow(i, power, p)) % p
        shares.append((i, accum))
    
    return shares

def lagrange_interpolate(x, x_s, y_s, p):
    total = 0
    for i in range(len(x_s)):
        xi, yi = x_s[i], y_s[i]
        li = 1
        for j in range(len(x_s)):
            if i == j:
                continue
            xj = x_s[j]
            li = (li * (x - xj) * mod_inverse(xi - xj, p)) % p
        total = (total + yi * li) % p
    return total

def reconstruct_secret(shares, p):
    x_s, y_s = zip(*shares)
    return lagrange_interpolate(0, x_s, y_s, p)

# Example usage:
if __name__ == "__main__":
    # A large prime number (should be greater than the secret)
    p = 711577331239842805114550041213069154795634469703966193517588669628016485152750041731176583762267136103285795860447322461808709838439645383515985384043692155718147949524715956189033281228535852472381177902070093705760092109751916910892196462236676848628418360675233448440819174569075425896999465165453
    secret = random.getrandbits(256)  # The secret to split (as an integer)
    n = 7  # Total number of shares
    t = 7  # Threshold to reconstruct the secret

    shares = split_secret(secret, n, t, p)

    random.shuffle(shares)  # Shuffle shares to simulate distribution
    print("Shares:", shares)

    # Reconstruct the secret using any t shares (here, using the first t shares)
    recovered_secret = reconstruct_secret(shares[:t], p)

    print("Original Secret:", secret, sep="\t")
    print("Reconstructed Secret:", recovered_secret, sep="\t")

    assert secret == recovered_secret, "Reconstructed secret does not match the original secret"
