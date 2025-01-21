sarah = false
msg = false

collection = sarah ? "EO:EUM:DAT:0863" : msg ? "EO:EUM:DAT:MSG:HRSEVIRI" : "EO:EUM:DAT:0662"
output_file = "products_$(sarah ? "sarah" : msg ? "msg" : "mtg").txt"

start_date = "2024-09-01T00:00:00"
end_date = "2025-02-07T00:00:00"

limit = 10_000

function getnextdate(product)
    if startswith(product, "SIS")
        years, months, days, hours, minutes, seconds =
            parse.(Int, getindex.((product,), [6:9, 10:11, 12:13, 14:15, 16:17, 18:19]))
        hours += 2
    elseif startswith(product, "MSG")
        a, _ = split(product, ".")
        time = last(split(a, "-"))
        years, months, days, hours, minutes, seconds =
            parse.(Int, getindex.((time,), [1:4, 5:6, 7:8, 9:10, 11:12, 13:14]))
    else
        time = split(product, "_")[end-5]
        years, months, days, hours, minutes, seconds =
            parse.(Int, getindex.((time,), [1:4, 5:6, 7:8, 9:10, 11:12, 13:14]))
    end
    minutes += 1
    if minutes >= 60
        hours += 1
        minutes %= 60
    end
    if hours >= 24
        days += 1
        hours %= 24
    end
    years, months, days, hours, minutes, seconds =
        lpad.((years, months, days, hours, minutes, seconds), 2, "0")
    "$(years)-$(months)-$(days)T$(hours):$(minutes):$(seconds)"
end

function eumdac(start_date, end_date)
    @assert !isnothing(match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$", start_date)) start_date
    @assert !isnothing(match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$", end_date)) end_date
    eumdac_bin = expanduser("~/BFH/eumetsat/eumdac")
    cmd = `$eumdac_bin search
             -c $collection
             --limit $limit
             --sort sensing --yes
             --time-range $start_date $end_date`
    if sarah
        push!(cmd.exec, "--product-type", "SIS")
    end
    out = read(cmd, String)
    products = split(out, "\n"; keepempty=false)
    if sarah
        filter!(p -> startswith(p, "SISin"),
                products)
    end
    @info "Fetching" n_products = length(products) start_date end_date
    return products
end

function getall(start_date, end_date)

    open(output_file, "w") do io
        while true
            products = eumdac(
                start_date, end_date
            )

            if isempty(products)
                break
            end

            for p in products
                println(io, p)
            end

            start_date = getnextdate(last(products))
        end
    end
end

# print(eumdac())
# @info getnextdate("MSG1-SEVI-MSG15-0201-NA-20050415141239.995000000Z-NA")
getall(start_date, end_date)
