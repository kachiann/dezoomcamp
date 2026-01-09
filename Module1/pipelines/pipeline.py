import sys  # Standard library for CLI args and more

print('arguments', sys.argv) # [ 'pipeline.py', '6' ] — argv[0] is script name
month = int(sys.argv[1])
print(f'hello pipeline!, month = {month}')