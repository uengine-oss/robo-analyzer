stack = []

def process_stack_commands(stack, commands):
    for command in commands.split("\n"):
        if not command.strip():
            continue
        parts = command.split()
        action = parts[0]
        if action == "PUSH":
            _, item, line = parts
            stack.append((item, line))
        elif action == "POP":
            stack.pop()  # Assuming the item to pop is always the last one added

    # Formatting the final stack state into a string with indentation
    # Formatting the final stack state into a string with indentation and parent information
    result = []
    for depth, (item, line) in enumerate(stack):
        indent = "  " * depth
        if depth == 0:  # First item is the root
            parent_info = " <- root"
        elif depth == len(stack) - 1:  # Last item before the current one
            parent_info = f" <- current parent"
        else:
            parent_info = ""
        result.append(f"{indent}+-   {item}{{id:{line}}}{parent_info}")

    return "\n".join(result)

# Example usage
commands = """
PUSH PROCEDURE 1
PUSH IF 4
PUSH SELECT 5
POP
PUSH IF 7
PUSH SELECT 8
POP
"""
print(process_stack_commands(stack, commands))

commands = """
POP
POP
"""

print(process_stack_commands(stack, commands))
