% Pid, M:F or M:F(A1, A2, ..., An)
-type callback() :: pid() |
                    fun() | 
                    {atom(), atom()} |
                    {atom(), atom(), [any()]}.

-type filters()  :: all | [atom()].

-type message_metadata() :: {done | incomplete, integer(), integer()}.