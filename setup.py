from distutils.core import setup

setup(name="dagman",
      version="0.1",
      description="Module to create DAGMan job and submit files",
      author="Thorben Menne",
      author_email="mennthor@aol.com",
      url="github.com/mennthor/dagman",
      packages=["dagman"],
      entry_points={
          "console_scripts": [
              "pbs_submitter_helper=dagman.pbs:_pbs_submitter_entrypoint",
          ],
      }
      )
