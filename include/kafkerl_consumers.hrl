% Pid, M:F or M:F(A1, A2, ..., An)
-type kafkerl_callback() :: pid() |
                            {atom(), atom()} |
                            {atom(), atom(), [any()]}.

-type kafkerl_message_metadata() :: {done | incomplete, integer(), integer()}.