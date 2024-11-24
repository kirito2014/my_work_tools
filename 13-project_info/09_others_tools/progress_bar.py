import time
import sys

def process_step(step_number):
    time.sleep(0.1)

def progress_bar(current,total,bar_length=50):
    progress=current / total
    block = int(bar_length * progress)
    percentage = progress * 100
    text = f"\rProgress: [{'#' * block}{'-' * (bar_length - block)}] {percentage:.2f}%"
    sys.stdout.write(text)
    sys.stdout.flush()
def main():
    total_steps = 100
    for i in range(total_steps):
        process_step(i + 1)
        progress_bar(i + 1, total_steps)
    print()
if __name__ == "__main__":
    main()