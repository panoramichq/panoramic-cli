from panoramic.cli.husky.core.tel.evaluator.ast import TelExpression
from panoramic.cli.husky.core.tel.evaluator.expressions import (
    TelStrictNumericOp,
    TelTaxon,
)


def _contains_taxon(node: TelExpression) -> bool:
    """Looks for taxon in the tree defined by the node"""
    if isinstance(node, TelTaxon):
        return True
    elif len(node.children) == 0:
        return False
    else:
        return any([_contains_taxon(child) for child in node.children])


def can_become_comparison_metric(node: TelExpression) -> bool:
    """
    Determines whether the calculation described by the AST can be used as a comparison metric
    """
    # look for division node
    if isinstance(node, TelStrictNumericOp):
        # either the left subtree contains division with taxon
        if can_become_comparison_metric(node.children[0]):
            return True

        # or the right subtree contains a taxon
        if _contains_taxon(node.children[1]):
            return True
    elif len(node.children):
        # if there are any children to look into
        return any([can_become_comparison_metric(child) for child in node.children])

    return False
