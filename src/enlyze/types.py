from typing import Sequence

from enlyze import models as user_models

VariableOrVariableWithResamplingMethodSequence = (
    Sequence[user_models.Variable] | Sequence[user_models.VariableWithResamplingMethod]
)
