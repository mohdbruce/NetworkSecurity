from setuptools import setup, find_packages
from typing import List

file_path = 'requirements.txt'

def get_requirements(file_path: str) -> List[str]:
    """Read the requirements from a file and return them as a list."""
    requirement_lst: List[str] = []
    try:
        with open(file_path, 'r') as file:
            #Read the file 
            line=file.readlines()
            #process each line
            for line in line:
                requirement=line.strip()
                if requirement and requirement!='-e .':
                    requirement_lst.append(requirement)
    except FileNotFoundError:
        print(f"The file {file_path} was not found.")

    return requirement_lst

setup(
    name='MLOps_Project',
    version='0.0.1',
    author='Mohd Bruce',
    packages=find_packages(),
    install_requires=get_requirements(file_path)
)