import pkg_resources

installed_packages = {d.project_name:d.version for d in pkg_resources.Working_set}
for package,version in installed_packages.items():
    print(f"{package}=={version}")