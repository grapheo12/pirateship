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
    p = 208351617316091241234326746312124448251235562226470491514186331217050270460481
    secret = 123456789  # The secret to split (as an integer)
    n = 5  # Total number of shares
    t = 3  # Threshold to reconstruct the secret

    shares = split_secret(secret, n, t, p)
    print("Shares:", shares)

    # Reconstruct the secret using any t shares (here, using the first t shares)
    recovered_secret = reconstruct_secret(shares[:t], p)
    print("Reconstructed Secret:", recovered_secret)
