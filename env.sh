# Ativa o ambiente local: source env.sh
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
# Usa o JDK 17 embarcado em .tools/ se existir; caso contrário, mantém o JAVA_HOME do sistema.
if [ -d "$PROJECT_DIR/.tools/jdk17/Contents/Home" ]; then
  export JAVA_HOME="$PROJECT_DIR/.tools/jdk17/Contents/Home"
  export PATH="$JAVA_HOME/bin:$PATH"
fi
source "$PROJECT_DIR/.venv/bin/activate"
