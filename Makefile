COOKIE ?= KAFKERL-EXAMPLE
CONFIG ?= rel/kafkerl.app.config
ERL ?= erl
RUN := ${ERL} -pa ebin -pa deps/*/ebin -smp enable -s lager -setcookie ${COOKIE} -config ${CONFIG} -boot start_sasl ${ERL_ARGS}
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

all:
	${REBAR} get-deps compile

quick:
	${REBAR} skip_deps=true compile

clean:
	${REBAR} clean

quick_clean:
	${REBAR} skip_deps=true clean

tests_clean:
	rm -rf log/ct

clean_logs:
	rm -rf log/*

${DIALYZER_OUT}:
	dialyzer --verbose --build_plt -pa deps/*/ebin --output_plt ${DIALYZER_OUT} \
	 --apps stdlib erts compiler crypto edoc gs syntax_tools tools runtime_tools \
	 inets xmerl ssl mnesia webtool kernel

analyze: quick ${DIALYZER_OUT} xref
	dialyzer --verbose --plt ${DIALYZER_OUT} -Werror_handling \
		`find ebin -name "choosy*.beam" | grep -v SUITE | grep -v choosy_test_data_generation` \
		`find deps/sumo_db/ebin -name "sumo*.beam" | grep -v SUITE` \
		| grep -e "[^:][^:]*:[0-9][0-9]*[:]" --color=always --context=10000 | tee /dev/tty \
		| grep -e "[^:][^:]*:[0-9][0-9]*[:]" | wc -l

quick_analyze: quick
	dialyzer --verbose --plt ${DIALYZER_OUT} -Werror_handling \
		`find ebin -name "choosy*.beam" | grep -v SUITE | grep -v choosy_test_data_generation` \
		`find deps/sumo_db/ebin -name "sumo*.beam" | grep -v SUITE` \
		| grep -e "[^:][^:]*:[0-9][0-9]*[:]" --color=always --context=10000 | tee /dev/tty \
		| grep -e "[^:][^:]*:[0-9][0-9]*[:]" | wc -l

xref:
	${REBAR} skip_deps=true --verbose compile xref

shell: quick
	if [ -n "${NODE}" ]; then ${RUN} -name ${NODE}@`hostname`; \
	else ${RUN}; \
	fi

run: quick
	if [ -n "${NODE}" ]; then ${RUN} -name ${NODE}@`hostname` -s kafkerl; \
	else ${RUN} -s kafkerl; \
	fi

test: tests

tests:
	@${REBAR} compile skip_deps=true
	@rm -rf ${CT_LOG}
	@mkdir -p ${CT_LOG}
	@ERL_FLAGS="${TEST_ERL_ARGS}" \
	ERL_AFLAGS="${TEST_ERL_ARGS}" \
	${REBAR} -v 3 skip_deps=true ${CT_SUITES_} ${CT_CASE_} ct
