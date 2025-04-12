import re
import sys

# Regular expression to capture word tokens
token_pattern = re.compile(r"\b\w+\b")


def tokenize_text(text):
    # Convert to lowercase and split into tokens
    return token_pattern.findall(text.lower())


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split("\t", 2)
        if len(parts) != 3:
            continue

        _, _, text = parts

        # Tokenization
        tokens = tokenize_text(text)

        # Ensure each unique token is returned only once per document
        unique_tokens = set(tokens)

        for token in unique_tokens:
            print(f"{token}\t1")


if __name__ == "__main__":
    main()
