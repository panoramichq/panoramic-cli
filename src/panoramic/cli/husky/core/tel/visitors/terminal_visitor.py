from panoramic.cli.tel_grammar.TelParser import TelParser as AntlrTelParser
from panoramic.cli.tel_grammar.TelVisitor import TelVisitor as AntlrTelVisitor


class TelTerminalVisitor(AntlrTelVisitor):
    """
    Visitor that will render all terminal nodes exactly as they are.
    """

    def __init__(self):
        self._output = []

    def visitTerminal(self, node):
        if node.symbol.type != AntlrTelParser.EOF:
            self._output.append(str(node))

    @property
    def output_string(self):
        return ''.join(self._output)
