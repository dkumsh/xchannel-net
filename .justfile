# print options
default:
    @just --list --unsorted

# install cargo tools
init:
    cargo upgrade --incompatible
    cargo update

# check code
check:
    cargo check
    cargo fmt --all -- --check
    cargo clippy --all-targets --all-features -- -D warnings
    # Rustdoc too, and with private items: this codebase's design record lives in its comments, so a
    # comment that links to something deleted is a defect. Clippy does not run rustdoc lints, so
    # `-D warnings` on clippy alone left that hole open — three deleted symbols were still being linked.
    RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --document-private-items --workspace

# list every place a lock guard outlives its own statement, for review
#
# The lock order on `Node` is a documented invariant with nothing enforcing it, and it has been broken
# twice — both times because a `MutexGuard` lived longer than it looked like it did. These are the
# shapes where that happens; most uses are fine, so this prints for a human to read rather than failing.
# Check each: does the body acquire another lock, or does the bound value *borrow* from the guard?
locks:
    @echo "--- guards in an if-let / match scrutinee (alive for the whole body) ---"
    @grep -rn 'if let .*lock_safe()\|match .*lock_safe()\|while let .*lock_safe()' --include='*.rs' xchannel-net xchannel-net-core xchannel-net-client || true
    @echo "--- guards bound to a name (alive to end of block; check for borrows) ---"
    @grep -rn 'let \(mut \)\?[a-z_]* = [a-z_.]*lock_safe()' --include='*.rs' xchannel-net xchannel-net-core xchannel-net-client || true
    @echo "--- guards in a for-head (alive for the whole loop) ---"
    @grep -rn 'for .* in .*lock_safe()' --include='*.rs' xchannel-net xchannel-net-core xchannel-net-client || true
    @echo "--- typed bindings and let-else scrutinees (a let-else guard dies BEFORE the else) ---"
    @grep -rn 'let [a-z_]*\(: [^=]*\)\? = .*lock_safe()\|else {' --include='*.rs' xchannel-net/src xchannel-net-core/src | grep lock_safe || true
    @echo "--- match guards taking a lock (a guard is its own temporary scope, but check the arm) ---"
    @grep -rn 'if .*lock_safe().* =>' --include='*.rs' xchannel-net/src xchannel-net-core/src || true
    @echo "--- multi-line guard bindings (check the following lines by hand) ---"
    @grep -rn -B1 '^\s*\.lock_safe()' --include='*.rs' xchannel-net xchannel-net-core xchannel-net-client | grep 'let ' || true

# automatically fix clippy warnings
fix:
    cargo fmt --all
    cargo clippy --allow-dirty --allow-staged --fix

# build project
build:
   cargo build --all-targets

# execute tests
test:
   cargo test
