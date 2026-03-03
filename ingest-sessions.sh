#!/usr/bin/env bash
set -euo pipefail

# ─── Configuration ────────────────────────────────────────────────────
HINDSIGHT_API="http://localhost:8888"
BANK_ID="openclaw"
OPENCLAW_HOME="$HOME/.openclaw"
BANK_MISSION="Memory system for Karen Hosova, AI assistant to Kavan Shaban. Stores conversation history, workspace context, system knowledge, decisions, and operational patterns."

# ─── Colors ───────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}═══════════════════════════════════════════════════${NC}"
echo -e "${GREEN} Hindsight Full Memory Ingestion${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════${NC}"
echo ""

# ─── Verify Hindsight API ────────────────────────────────────────────
echo -n "Checking Hindsight API... "
if ! curl -sf "$HINDSIGHT_API/health" > /dev/null 2>&1; then
    echo -e "${RED}FAILED${NC}"
    echo "Hindsight API is not reachable at $HINDSIGHT_API"
    exit 1
fi
echo -e "${GREEN}OK${NC}"

# ─── Ensure bank exists (PUT for this API version) ───────────────────
echo -n "Ensuring bank '$BANK_ID' exists... "
curl -sf -X PUT "$HINDSIGHT_API/v1/default/banks/$BANK_ID" \
    -H "Content-Type: application/json" \
    -d "{\"mission\": \"$BANK_MISSION\"}" > /dev/null 2>&1 || true
echo -e "${GREEN}OK${NC}"
echo ""

# ─── Discover sessions ───────────────────────────────────────────────
TOTAL_FILES=0
TOTAL_INGESTED=0
TOTAL_SKIPPED=0
TOTAL_ERRORS=0

# Find all agent directories
for AGENT_DIR in "$OPENCLAW_HOME"/agents/*/sessions; do
    if [ ! -d "$AGENT_DIR" ]; then
        continue
    fi
    
    AGENT_ID=$(basename "$(dirname "$AGENT_DIR")")
    SESSION_FILES=( "$AGENT_DIR"/*.jsonl )
    
    # Skip if no jsonl files
    if [ ! -f "${SESSION_FILES[0]}" ]; then
        continue
    fi
    
    FILE_COUNT=${#SESSION_FILES[@]}
    echo -e "${YELLOW}Agent: $AGENT_ID${NC} — $FILE_COUNT session file(s)"
    
    for SESSION_FILE in "${SESSION_FILES[@]}"; do
        # Skip lock files
        if [[ "$SESSION_FILE" == *.lock ]]; then
            continue
        fi
        
        FILENAME=$(basename "$SESSION_FILE")
        SESSION_UUID="${FILENAME%.jsonl}"
        TOTAL_FILES=$((TOTAL_FILES + 1))
        
        # Extract session metadata from first line
        FIRST_LINE=$(head -1 "$SESSION_FILE" 2>/dev/null || echo "{}")
        SESSION_TYPE=$(echo "$FIRST_LINE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('type',''))" 2>/dev/null || echo "")
        SESSION_TS=$(echo "$FIRST_LINE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('timestamp',''))" 2>/dev/null || echo "")
        
        if [ "$SESSION_TYPE" != "session" ]; then
            echo "  ⏭ $FILENAME — not a session file (type: $SESSION_TYPE)"
            TOTAL_SKIPPED=$((TOTAL_SKIPPED + 1))
            continue
        fi
        
        # Build conversation text from messages
        CONVERSATION=$(python3 - "$SESSION_FILE" "$AGENT_ID" << 'PYEOF'
import json, sys

session_file = sys.argv[1]
agent_id = sys.argv[2]

messages = []
with open(session_file, 'r') as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        
        if obj.get('type') != 'message':
            continue
        
        msg = obj.get('message', {})
        role = msg.get('role', '')
        
        if role not in ('user', 'assistant'):
            continue
        
        # Extract text content
        content = msg.get('content', '')
        if isinstance(content, list):
            text_parts = []
            for part in content:
                if isinstance(part, dict) and part.get('type') == 'text':
                    text_parts.append(part.get('text', ''))
            content = '\n'.join(text_parts)
        
        if not content or len(content.strip()) < 5:
            continue
        
        # Skip hindsight_memories injection blocks
        if content.startswith('<hindsight_memories>'):
            if 'User message:' in content:
                content = content.split('User message:', 1)[1].strip()
            else:
                continue
        
        # Strip metadata blocks
        if content.startswith('Conversation info'):
            lines = content.split('\n')
            in_json = False
            real_content = []
            for l in lines:
                if '```json' in l:
                    in_json = True
                    continue
                if '```' in l and in_json:
                    in_json = False
                    continue
                if not in_json and not l.startswith('Conversation info'):
                    real_content.append(l)
            content = '\n'.join(real_content).strip()
        
        # Truncate very long messages
        if len(content) > 5000:
            content = content[:5000] + '\n[... truncated ...]'
        
        prefix = "User" if role == "user" else "Assistant"
        messages.append(f"{prefix}: {content}")

if messages:
    output = f"Session with agent {agent_id}\n\n"
    output += '\n\n'.join(messages)
    print(output)
else:
    print("")
PYEOF
)
        
        if [ -z "$CONVERSATION" ] || [ ${#CONVERSATION} -lt 50 ]; then
            echo "  ⏭ $FILENAME — empty or too short"
            TOTAL_SKIPPED=$((TOTAL_SKIPPED + 1))
            continue
        fi
        
        # Ingest into Hindsight using POST /memories with items array
        export INGEST_CONTENT="$CONVERSATION"
        RESPONSE=$(python3 - "$AGENT_ID" "$SESSION_UUID" "$SESSION_TS" << PYEOF
import json, sys, os, urllib.request

content = os.environ.get('INGEST_CONTENT', '')
agent_id = sys.argv[1]
session_id = sys.argv[2]
timestamp = sys.argv[3]

payload = json.dumps({
    "items": [{
        "content": content,
        "metadata": {
            "source": "session-ingestion",
            "agent_id": agent_id,
            "session_id": session_id,
            "timestamp": timestamp
        }
    }]
}).encode()

req = urllib.request.Request(
    "http://localhost:8888/v1/default/banks/openclaw/memories",
    data=payload,
    headers={"Content-Type": "application/json"},
    method="POST"
)

try:
    with urllib.request.urlopen(req, timeout=300) as resp:
        result = json.loads(resp.read())
        print(json.dumps({"ok": True, "usage": result.get("usage", {})}))
except Exception as e:
    print(json.dumps({"ok": False, "error": str(e)}))
PYEOF
) 2>&1 || {
            echo -e "  ${RED}✗${NC} $FILENAME — script error"
            TOTAL_ERRORS=$((TOTAL_ERRORS + 1))
            continue
        }
        
        IS_OK=$(echo "$RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('ok',False))" 2>/dev/null || echo "False")
        
        if [ "$IS_OK" = "True" ]; then
            CHAR_COUNT=${#CONVERSATION}
            echo -e "  ${GREEN}✓${NC} $FILENAME — ingested (${CHAR_COUNT} chars, ts: ${SESSION_TS:0:10})"
            TOTAL_INGESTED=$((TOTAL_INGESTED + 1))
        else
            ERROR_MSG=$(echo "$RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('error','unknown'))" 2>/dev/null || echo "unknown")
            echo -e "  ${RED}✗${NC} $FILENAME — API error: $ERROR_MSG"
            TOTAL_ERRORS=$((TOTAL_ERRORS + 1))
        fi
        
        # Small delay to avoid overwhelming the API
        sleep 2
    done
    echo ""
done

# ─── Summary ─────────────────────────────────────────────────────────
echo -e "${GREEN}═══════════════════════════════════════════════════${NC}"
echo -e "${GREEN} Ingestion Complete${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════${NC}"
echo ""
echo " Total session files found: $TOTAL_FILES"
echo -e " ${GREEN}Ingested:${NC} $TOTAL_INGESTED"
echo -e " ${YELLOW}Skipped:${NC} $TOTAL_SKIPPED"
echo -e " ${RED}Errors:${NC} $TOTAL_ERRORS"
echo ""

# ─── Gate check ───────────────────────────────────────────────────────
if [ "$TOTAL_ERRORS" -gt 0 ]; then
    echo -e "${RED}⚠️  There were errors during ingestion.${NC}"
    echo -e "${RED}  Fix errors and re-run before proceeding to Phase 5.${NC}"
    exit 1
fi

if [ "$TOTAL_INGESTED" -eq 0 ]; then
    echo -e "${RED}⚠️  No sessions were ingested. Check OPENCLAW_HOME path.${NC}"
    exit 1
fi

# Verify bank has memories
echo -n "Verifying memory bank... "
STATS=$(curl -sf "http://localhost:8888/v1/default/banks/openclaw/stats" 2>/dev/null || echo "{}")
echo "$STATS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'Bank openclaw — {d.get(\"memory_unit_count\",\"?\")} memory units, {d.get(\"entity_count\",\"?\")} entities')" 2>/dev/null || echo "Could not fetch stats"
echo ""
echo -e "${GREEN}✅ Ingestion succeeded. Safe to proceed to Phase 5 (OpenClaw Configuration).${NC}"
