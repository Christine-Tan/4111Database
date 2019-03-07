class PrimaryKeyError:
    def __init__(self, key, msg):
        self.key = key;
        self.msg = msg;

    def __str__(self):
        return "Error primary key is:" + str(self.key) + "Error Msg:" + self.msg;
