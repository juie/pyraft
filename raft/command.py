class Command(object):
    def __init__(self, command_string):
        self.command_string = command_string


class NOPCommand(Command):
    def __init__(self, command_string=None):
        super(NOPCommand, self).__init__(command_string)


class StopLeader(Command):
    def __init__(self, command_string="StopLeader"):
        super(StopLeader, self).__init__(command_string)


class DefaultJoinCommand(Command):
    def __init__(self, name, connect_string, command_string="DefaultJoinCommand"):
        super(DefaultJoinCommand, self).__init__(command_string)
        self.name = name
        self.connect_string = connect_string


class SelfJoinCommand(Command):
    def __init__(self, command_string="SelfJoinCommand"):
        super(SelfJoinCommand, self).__init__(command_string)
        # self.connect_string = connect_string
