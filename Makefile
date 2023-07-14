.PHONY: checksetup

checksetup:
	conda info --envs \
	&& echo $$(which python)
