#!/usr/bin/env python3

from aws_cdk import core

from cdkpy.cdkpy_stack import CdkpyStack


app = core.App()
CdkpyStack(app, "cdkpy")

app.synth()
