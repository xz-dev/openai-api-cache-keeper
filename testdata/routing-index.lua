-- Run in the APISIX image with the deployed selector path and shared JSON fixture.
local home = os.getenv("APISIX_HOME") or "/usr/local/apisix"
package.path = home .. "/?.lua;" .. home .. "/deps/share/lua/5.1/?.lua;" ..
    home .. "/deps/share/lua/5.1/?/init.lua;" .. package.path
package.cpath = home .. "/deps/lib/lua/5.1/?.so;" ..
    home .. "/deps/lib64/lua/5.1/?.so;" .. package.cpath
local json = require("cjson.safe")
local fixture = assert(io.open(arg[2], "rb"))
local cases = assert(json.decode(fixture:read("*a")))
fixture:close()
for _, case in ipairs(cases) do
    local writes, deletes, calls = 0, 0, 0
    _G.ngx = {
        req = {
            read_body = function() end,
            get_body_data = function() return '{"model":"fixture"}' end,
            set_header = function() end,
        },
        shared = {routing_lkg = {
            get = function() return "cpa" end,
            set = function() writes = writes + 1 end,
            delete = function() deletes = deletes + 1 end,
        }},
        escape_uri = function(v) return v end,
        log = function() end,
        WARN = 5,
    }
    package.loaded["resty.http"] = {new = function()
        return {
            set_timeouts = function() end,
            connect = function() return true end,
            request = function()
                calls = calls + 1
                local sent = false
                return {
                    status = 200,
                    headers = {["Content-Length"] = tostring(#case.body)},
                    body_reader = function()
                        if sent then return nil end
                        sent = true
                        return case.body
                    end,
                }
            end,
            close = function() end,
            set_keepalive = function() end,
        }
    end}
    local selector = dofile(arg[1])
    local ctx = {}
    local status = selector.run(nil, ctx, "fixture-key")
    assert(calls == 1, case.name .. ": lookup count")
    if case.decision == "" then
        assert(status == nil and ctx.upstream_id == "cpa" and writes == 0 and deletes == 0,
            case.name .. ": malformed response was accepted")
    elseif case.decision == "cpa" or case.decision == "aisix" then
        assert(status == nil and ctx.upstream_id == case.decision and writes == 1 and deletes == 0,
            case.name .. ": backend mismatch")
    else
        local expected = case.decision == "not_found" and 404 or 503
        assert(status == expected and ctx.upstream_id == nil and writes == 0 and deletes == 1,
            case.name .. ": authoritative negative mismatch")
    end
    print("PASS " .. case.name)
end
print("PASS shared routing-index fixture: " .. #cases)
