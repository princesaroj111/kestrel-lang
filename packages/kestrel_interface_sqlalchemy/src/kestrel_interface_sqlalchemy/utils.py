import inspect


def iter_argument_from_function_in_callstack(func_name: str, arg_name: str):
    
    stack_height = len(inspect.stack())

    # search for the first func in callstack
    # skip this function (starting from layer 1)
    for layer in range(1, stack_height):
        if inspect.stack()[layer][3] == func_name:
            break

    # yield arg
    while inspect.stack()[layer][3] == func_name and layer < stack_height:
        yield inspect.stack()[i][0].f_locals[arg_name]
        layer += 1
