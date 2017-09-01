import sys
import time

print("Args are:")
for arg in sys.argv[1:]:
    print(arg)
print("Sleeping")
time.sleep(1)
print("Done")
