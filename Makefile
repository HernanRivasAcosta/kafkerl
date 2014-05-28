COOKIE ?= KAFKERL-EXAMPLE
CONFIG ?= rel/kafkerl.app.config
RUN := erl -pa ebin -pa deps/*/ebin -smp enable -s lager -s kafkerl -setcookie ${COOKIE} -config ${CONFIG} -boot start_sasl ${ERL_ARGS}
NODE ?= kafkerl
CT_ARGS ?= "-vvv"
CT_LOG ?= /logs/ct
ERLARGS=-pa ${DEPS} -pa ${APPS} -smp enable -boot start_sasl -args_file ${VM_ARGS} -s lager -s redis_config
TEST_ERL_ARGS ?= ${ERLARGS} -args_file ${TEST_VM_ARGS} -config ${TEST_CONFIG}
REBAR ?= "rebar"

ifdef CT_SUITES
	CT_SUITES_="suites=${CT_SUITES}"
else
	CT_SUITES_=""
endif
ifdef CT_CASE
	CT_CASE_="case=${CT_CASE}"
else
	CT_CASE_=""
endif

clean:
	${REBAR} clean

quick:
	${REBAR} skip_deps=true compile

shell: quick
	if [ -n "${NODE}" ]; then ${RUN} -name ${NODE}@`hostname`; \
	else ${RUN}; \
	fi

test: tests

tests:
	@${REBAR} compile skip_deps=true
	@rm -rf ${CT_LOG}
	@mkdir -p ${CT_LOG}
	@ERL_FLAGS="${TEST_ERL_ARGS}" \
	ERL_AFLAGS="${TEST_ERL_ARGS}" \
	${REBAR} -v 3 skip_deps=true ${CT_SUITES_} ${CT_CASE_} ct