from utils import run_vm

import types
import dis
import sys
import builtins
import operator
import collections
import inspect

Block = collections.namedtuple('Block', 'type, handler, level')


class Frame(object):
    def __init__(self, code, prev_frame, global_names, local_names):
        self.code = code
        self.prev_frame = prev_frame
        self.stack = []
        self.global_names = global_names
        self.local_names = local_names

        parsed_commands = dis.get_instructions(self.code)
        self.instructions_list = [
            (i.opname, i.argval, i.offset, i.opcode)
            for i in parsed_commands
        ]

        if prev_frame:
            self.builtin_names = prev_frame.builtin_names
        else:
            self.builtin_names = local_names['__builtins__']
            if hasattr(self.builtin_names, '__dict__'):
                self.builtin_names = self.builtin_names.__dict__
        self.last_instruction = 0
        self.block_stack = []


class Function(object):
    def __init__(self, name, code, global_names, args, vm):
        self.name = self.__name__ = name or code.co_name
        self.code = code
        self.global_names = global_names
        self.local_names = vm.frame.local_names
        self.args = tuple(args)
        self.vm = vm
        python_funcs = {'argdefs': self.args}
        self.func = types.FunctionType(code, global_names, **python_funcs)

    def __call__(self, *args, **kwargs):
        call_args = inspect.getcallargs(self.func, *args, **kwargs)
        func_frame = self.vm.make_frame(
            self.code, call_args, self.global_names, {}
        )
        result = self.vm.run_frame(func_frame)
        return result


class VirtualMachine(object):
    def __init__(self):
        self.frames = []
        self.frame = None
        self.return_value = None
        self.last_exception = None

    def run_code(self, code, global_names=None, local_names=None):
        frame = self.make_frame(code, global_names=global_names,
                                local_names=local_names)
        self.run_frame(frame)

    def make_frame(self, code, callargs={}, global_names=None,
                   local_names=None):
        if global_names is not None and local_names is not None:
            local_names = global_names
        elif self.frames:
            global_names = self.frame.global_names
            local_names = {}
        else:
            global_names = local_names = {
                '__builtins__': __builtins__,
                '__name__': '__main__',
                '__doc__': None,
                '__package__': None,
            }
        local_names.update(callargs)
        frame = Frame(code, self.frame, global_names, local_names)
        return frame

    def run_frame(self, frame):
        self.push_frame(frame)

        while frame.last_instruction < len(frame.instructions_list):
            instruction = frame.instructions_list[frame.last_instruction]
            frame.last_instruction += 1

            command = instruction[0]
            arg = instruction[1]
            key_word = None
            if command.startswith('UNARY_'):
                self.unaryOperator(command[6:])
            elif command.startswith('BINARY_'):
                self.binaryOperator(command[7:])
            elif command.startswith('INPLACE_'):
                self.binaryOperator(command[8:])
            else:
                bytecode_method = getattr(self, command, None)
                if arg is None:
                    key_word = bytecode_method()
                else:
                    key_word = bytecode_method(arg)

            while key_word and frame.block_stack:
                key_word = self.block_managing(key_word)

            if key_word:
                break
        self.pop_frame()

        return self.return_value

    def push_frame(self, frame):
        self.frames.append(frame)
        self.frame = frame

    def pop_frame(self):
        self.frames.pop()
        if self.frames:
            self.frame = self.frames[-1]
        else:
            self.frame = None

    def top(self):
        return self.frame.stack[-1]

    def peek(self, depth):
        return self.frame.stack[-depth]

    def pop(self):
        return self.frame.stack.pop()

    def pop_n(self, n):
        if not n:
            return []
        elements = self.frame.stack[-n:]
        self.frame.stack[-n:] = []
        return elements

    def push(self, *vals):
        self.frame.stack.extend(vals)

    # bytecode_commands:
    def NOP(self):
        return

    def PRINT_EXPR(self):
        print(self.pop())

    # loading
    def LOAD_CONST(self, const=None):
        self.push(const)

    def LOAD_FAST(self, name):
        if name in self.frame.local_names:
            val = self.frame.local_names[name]
        else:
            raise UnboundLocalError(
                'local variable {} referenced before assignment'.format(name)
            )
        self.push(val)

    def LOAD_GLOBAL(self, name):
        frame = self.frame
        if name in frame.global_names:
            val = frame.global_names[name]
        elif name in frame.builtin_names:
            val = frame.builtin_names[name]
        else:
            raise NameError(
                'global name {} is not defined'.format(name)
            )
        self.push(val)

    def LOAD_NAME(self, name):
        frame = self.frame
        if name in frame.local_names:
            val = frame.local_names[name]
        elif name in frame.global_names:
            val = frame.global_names[name]
        elif name in frame.builtin_names:
            val = frame.builtin_names[name]
        else:
            raise NameError('name {} is not defined'.format(name))
        self.push(val)

    # storing
    def STORE_NAME(self, name):
        self.frame.local_names[name] = self.pop()

    def STORE_FAST(self, name):
        self.frame.local_names[name] = self.pop()

    def STORE_ATTR(self, name):
        value, key = self.pop_n(2)
        setattr(key, name, value)

    def STORE_GLOBAL(self, name):
        self.frame.global_names[name] = self.pop()

    def STORE_SUBSCR(self):
        value, obj, subscr = self.pop_n(3)
        obj[subscr] = value

    # rotting
    def ROT_TWO(self):
        a, b = self.pop_n(2)
        self.push(b, a)

    def ROT_THREE(self):
        a, b, c = self.pop_n(3)
        self.push(c, a, b)

    # deleting
    def DELETE_GLOBAL(self, name):
        del self.frame.global_names[name]

    def DELETE_FAST(self, name):
        del self.frame.local_names[name]

    def DELETE_NAME(self, name):
        del self.frame.local_names[name]

    def DELETE_ATTR(self, name):
        object = self.pop()
        delattr(object, name)

    def DELETE_SUBSCR(self):
        object, subscr = self.pop_n(2)
        del object[subscr]

    # duping
    def DUP_TOP(self):
        val = self.pop()
        self.push(val, val)

    def DUP_TOP_TWO(self):
        val1, val2 = self.pop_n(2)
        self.push(val1, val2, val1, val2)

    # smth
    def POP_TOP(self):
        return self.pop()

    def IMPORT_FROM(self, namei):
        obj = self.top()
        self.push(getattr(obj, namei))

    def IMPORT_NAME(self, namei):
        frame = self.frame
        height, address = self.pop_n(2)
        self.push(__import__(namei, frame.global_names,
                             frame.local_names, address, height))

    def LOAD_ATTR(self, attr):
        obj = self.pop()
        value = getattr(obj, attr)
        self.push(value)

    def UNPACK_SEQUENCE(self, count):
        sequence = self.pop()
        for i in reversed(sequence):
            self.push(i)

    # building
    def BUILD_SLICE(self, count):
        if count == 2:
            a, b = self.pop_n(2)
            self.push(slice(a, b))
        elif count == 3:
            a, b, c = self.pop_n(3)
            self.push(slice(a, b, c))
        else:
            raise TypeError(
                'slice expected at most 3 arguments, got {}'.format(count)
            )

    def BUILD_LIST(self, size):
        list_ = self.pop_n(size)
        self.push(list_)

    def BUILD_TUPLE(self, size):
        tuple_ = tuple(self.pop_n(size))
        self.push(tuple_)

    def BUILD_SET(self, size):
        set_ = set(self.pop_n(size))
        self.push(set_)

    def BUILD_MAP(self, size):
        map_ = {}
        items = self.pop_n(2 * size)
        for i in range(size):
            map_[items[2 * i]] = items[2 * i + 1]
        self.push(map_)

    def BUILD_STRING(self, size):
        items = self.pop_n(size)
        string_ = ''
        for i in items:
            string_ += i
        self.push(string_)

    def BUILD_CONST_KEY_MAP(self, size):
        keys = self.pop()
        values = self.pop_n(size)
        map_ = {key: value for key, value in zip(keys, values)}
        self.push(map_)

    def BUILD_LIST_UNPACK(self, size):
        items = self.pop_n(size)
        list_ = []
        for item in items:
            for i in item:
                list_.append(i)
        self.push(list_)

    def BUILD_TUPLE_UNPACK(self, size):
        items = self.pop_n(size)
        list_ = []
        for item in items:
            for i in item:
                list_.append(i)
        self.push(tuple(list_))

    def BUILD_SET_UNPACK(self, size):
        items = self.pop_n(size)
        list_ = []
        for item in items:
            for i in item:
                list_.append(i)
        self.push(set(list_))

    def BUILD_MAP_UNPACK(self, size):
        items = self.pop_n(size)
        map_ = {}
        for item in items:
            for key in item.keys():
                map_[key] = item[key]
        return map_

    # structures operations
    def LIST_APPEND(self, depth):
        value = self.pop()
        list_ = self.peek(depth)
        list_.append(value)

    def MAP_ADD(self, depth):
        value, key = self.pop_n(2)
        map_ = self.peek(depth)
        map_[key] = value

    def SET_ADD(self, depth):
        value = self.pop()
        set_ = self.peek(depth)
        set_.add(value)

    # blocks
    def push_block(self, block_type, handler=None):
        height = len(self.frame.stack)
        self.frame.block_stack.append(Block(block_type, handler, height))

    def pop_block(self):
        return self.frame.block_stack.pop()

    def block_managing(self, key_word):
        block = self.frame.block_stack[-1]
        if block.type == 'loop' and key_word == 'continue':
            self.jump(self.return_value)
            key_word = None
            return key_word

        self.pop_block()
        while len(self.frame.stack) > block.level:
            self.pop()

        if block.type == 'loop' and key_word == 'break':
            self.jump(block.handler)
            key_word = None
            return key_word

        return key_word

    # loops
    def SETUP_LOOP(self, delta):
        self.push_block('loop', delta)

    def BREAK_LOOP(self):
        return 'break'

    def CONTINUE_LOOP(self, target):
        self.return_value = target
        return 'continue'

    def CONTINUE_LOOP(self, delta):
        self.return_value = delta
        return 'continue'

    def POP_BLOCK(self):
        self.pop_block()

    # functions
    def MAKE_FUNCTION(self, argc):
        name = self.pop()
        code = self.pop()
        args = self.pop_n(argc)
        global_names = self.frame.global_names
        func = Function(name, code, global_names, args, self)
        self.push(func)

    # jumps
    def jump(self, jump_to):
        number = 0
        frame = self.frame
        for i, instruction in enumerate(frame.instructions_list):
            if instruction[2] == jump_to:
                number = i
        self.frame.last_instruction = number

    def JUMP_FORWARD(self, jump_to):
        self.jump(jump_to)

    def JUMP_ABSOLUTE(self, jump_to):
        self.jump(jump_to)

    def JUMP_IF_FALSE_OR_POP(self, jump_to):
        val = self.top()
        if not val:
            self.jump(jump_to)
        else:
            self.pop()

    def JUMP_IF_TRUE_OR_POP(self, jump_to):
        val = self.top()
        if val:
            self.jump(jump_to)
        else:
            self.pop()

    def POP_JUMP_IF_FALSE(self, jump_to):
        val = self.pop()
        if not val:
            self.jump(jump_to)

    def POP_JUMP_IF_TRUE(self, jump_to):
        val = self.pop()
        if val:
            self.jump(jump_to)

    def GET_ITER(self):
        self.push(iter(self.pop()))

    def FOR_ITER(self, jump_to):
        iterator = self.top()
        try:
            next_val = next(iterator)
            self.push(next_val)
        except StopIteration:
            self.pop()
            self.jump(jump_to)

    # function calls
    def RETURN_VALUE(self):
        top = self.pop()

    def CALL_FUNCTION(self, arg_number):
        self.call_function(arg_number, [], {})

    def CALL_FUNCTION_KW(self, arg):
        kwargs_keys = self.pop()
        vals = [self.pop() for _ in range(len(kwargs_keys))]
        kwargs = {key: value for key, value in zip(kwargs_keys, vals)}
        self.call_function(arg - len(kwargs_keys), [], kwargs)

    def call_function(self, arg_len, args, kwargs):
        kwargs_number, args_number = divmod(arg_len, 256)
        dict_kwargs = {}
        for _ in range(kwargs_number):
            key, value = self.pop_n(2)
            dict_kwargs[key] = value
        dict_kwargs.update(kwargs)
        list_args = self.pop_n(args_number)
        list_args.extend(args)
        func = self.pop()
        value = func(*list_args, **dict_kwargs)
        self.push(value)

    # unary_operators
    UNARY_OPERATORS = {
        'POSITIVE': operator.pos,
        'NEGATIVE': operator.neg,
        'NOT': operator.not_,
        'INVERT': operator.invert
    }

    def unaryOperator(self, operation):
        arg = self.pop()
        self.push(self.UNARY_OPERATORS[operation](arg))

    # inplace and binary operators
    BINARY_OPERATORS = {
        'ADD': operator.add,
        'AND': operator.and_,
        'FLOOR_DIVIDE': operator.floordiv,
        'LSHIFT': operator.lshift,
        'MODULO': operator.mod,
        'MULTIPLY': operator.mul,
        'OR': operator.or_,
        'POWER': operator.pow,
        'RSHIFT': operator.rshift,
        'SUBSCR': operator.getitem,
        'SUBTRACT': operator.sub,
        'TRUE_DIVIDE': operator.truediv,
        'XOR': operator.xor
    }

    def binaryOperator(self, operation, flag=None):
        arg1, arg2 = self.pop_n(2)
        result = self.BINARY_OPERATORS[operation](arg1, arg2)
        self.push(result)

    # comparing
    COMPARE_OPERATORS = {
        '<': operator.lt,
        '<=': operator.le,
        '==': operator.eq,
        '!=': operator.ne,
        '>': operator.gt,
        '>=': operator.ge,
        'in': lambda x, y: x in y,
        'not in': lambda x, y: x not in y,
        'is': lambda x, y: x is y,
        'is not': lambda x, y: x is not y
    }

    def COMPARE_OP(self, operation):
        arg1, arg2 = self.pop_n(2)
        self.push(self.COMPARE_OPERATORS[operation](arg1, arg2))


if __name__ == '__main__':
    vm = VirtualMachine()
    run_vm(vm)
