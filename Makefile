COOKIE ?= KAFKERL-EXAMPLE
CONFIG ?= rel/kafkerl.app.config
RUN := erl -pa ebin -pa deps/*/ebin -smp enable -s lager -s kafkerl -setcookie ${COOKIE} -config ${CONFIG} -boot start_sasl ${ERL_ARGS}
NODE ?= kafkerl
CT_ARGS ?= "-vvv"
REBAR ?= "rebar"

all:
	${REBAR} get-deps compile

quick:
	${REBAR} skip_deps=true compile

clean:
	${REBAR} clean

quick_clean:
	${REBAR} skip_deps=true clean

clean_logs:
	rm -rf log/*

shell: quick
	if [ -n "${NODE}" ]; then ${RUN} -name ${NODE}@`hostname`; \
	else ${RUN}; \
	fi

run: quick
	if [ -n "${NODE}" ]; then ${RUN} -name ${NODE}@`hostname` -s kafkerl; \
	else ${RUN} -s kafkerl; \
	fi