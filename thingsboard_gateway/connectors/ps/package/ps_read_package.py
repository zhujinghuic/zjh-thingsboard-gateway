from thingsboard_gateway.connectors.ps.package.ps_package import PsPackage


class PsReadPackage(PsPackage):

    def __init__(self, psFrameEnum) -> None:
        super().__init__(psFrameEnum)