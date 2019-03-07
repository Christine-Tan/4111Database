class ValueError:
    def __init__(self, value, msg):
        self.key = value;
        self.msg = msg;

    def __str__(self):
        return "Error primary key is:" + str(self.key) + "Error Msg:" + self.msg;