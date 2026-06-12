with open("src/ring_buffer.py", "r") as f:
    code = f.read()

code = code.replace("""    def close(self):
        if self.shm is not None:
            self.shm.close()
            self.shm = None

    def unlink(self):
        if self.shm is not None:
            try:
                self.shm.unlink()
            except FileNotFoundError:
                pass""", """    def close(self):
        if self.shm is not None:
            self.shm.close()

    def unlink(self):
        if self.shm is not None:
            try:
                self.shm.unlink()
            except FileNotFoundError:
                pass""")

with open("src/ring_buffer.py", "w") as f:
    f.write(code)
