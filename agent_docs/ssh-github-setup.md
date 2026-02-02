# Подключение к GitHub через SSH — пошаговая инструкция

Пошаговые действия для настройки SSH-доступа к репозиторию [pass24-knowlege-base](https://github.com/akhromovRT/pass24-knowlege-base). Выполняйте команды в терминале по порядку.

---

## Шаг 1: Проверить, есть ли уже SSH-ключ

Откройте терминал и выполните:

```bash
ls -la ~/.ssh
```

**Если папка есть и в ней есть файлы вида `id_ed25519` и `id_ed25519.pub` (или `id_rsa` и `id_rsa.pub`)** — ключ уже есть, переходите к **шагу 3**.

**Если папки `~/.ssh` нет или в ней нет ключей** — переходите к **шагу 2**.

---

## Шаг 2: Создать папку .ssh и сгенерировать ключ

Выполните по очереди:

```bash
mkdir -p ~/.ssh
chmod 700 ~/.ssh
```

Затем создайте ключ (подставьте свой email вместо примера):

```bash
ssh-keygen -t ed25519 -C "ваш_email@example.com" -f ~/.ssh/id_ed25519 -N ""
```

Флаг `-N ""` задаёт пустую парольную фразу (при желании пароль можно задать позже через `ssh-keygen -p`).

Проверьте, что ключи появились:

```bash
ls -la ~/.ssh
```

Должны быть файлы: `id_ed25519` (приватный) и `id_ed25519.pub` (публичный).

---

## Шаг 3: Добавить ключ в ssh-agent (рекомендуется)

На macOS:

```bash
eval "$(ssh-agent -s)"
ssh-add --apple-use-keychain ~/.ssh/id_ed25519
```

Если ключ был создан с паролем, введите его по запросу. Так ключ будет подхватываться при работе с GitHub.

---

## Шаг 4: Добавить публичный ключ в GitHub

1. Скопируйте содержимое публичного ключа в буфер обмена:

   ```bash
   pbcopy < ~/.ssh/id_ed25519.pub
   ```

   (Если `pbcopy` недоступен, выведите ключ и скопируйте вручную: `cat ~/.ssh/id_ed25519.pub`.)

2. Откройте в браузере: **https://github.com/settings/keys**
3. Нажмите **New SSH key**.
4. Заполните:
   - **Title** — например: `MacBook` или `Cursor`.
   - **Key type** — Authentication Key.
   - **Key** — вставьте из буфера обмена (Ctrl+V / Cmd+V).
5. Нажмите **Add SSH key**.

---

## Шаг 5: Добавить GitHub в known_hosts

Чтобы при первом подключении не возникала ошибка «Host key verification failed»:

```bash
ssh-keyscan -t ed25519,rsa github.com >> ~/.ssh/known_hosts 2>/dev/null
chmod 600 ~/.ssh/known_hosts
```

Если файла `~/.ssh/known_hosts` ещё не было, он будет создан.

---

## Шаг 6: Проверить подключение к GitHub

Выполните:

```bash
ssh -T git@github.com
```

При первом подключении может появиться вопрос о доверии хосту — введите `yes`.

Ожидаемый ответ:

```
Hi akhromovRT! You've successfully authenticated, but GitHub does not provide shell access.
```

Если видите такое сообщение — SSH настроен верно.

---

## Шаг 7: Настроить remote репозитория на SSH

Перейдите в папку проекта и проверьте remote:

```bash
cd "/Users/akhromov/Library/Mobile Documents/com~apple~CloudDocs/Cursor/База знаний PASS24"
git remote -v
```

Должно быть:

```
origin  git@github.com:akhromovRT/pass24-knowlege-base.git (fetch)
origin  git@github.com:akhromovRT/pass24-knowlege-base.git (push)
```

Если вместо этого указан адрес вида `https://github.com/akhromovRT/pass24-knowlege-base.git`, переключите на SSH:

```bash
git remote set-url origin git@github.com:akhromovRT/pass24-knowlege-base.git
git remote -v
```

Убедитесь, что в обеих строках используется `git@github.com:...`.

---

## Шаг 8: Проверить push

Отправьте изменения на GitHub:

```bash
git push origin main
```

Если всё настроено правильно, команда выполнится без ошибок «Host key verification failed» и «Permission denied».

---

## Краткая последовательность (если ключа ещё не было)

| № | Действие | Команда / место |
|---|----------|------------------|
| 1 | Создать `~/.ssh` | `mkdir -p ~/.ssh && chmod 700 ~/.ssh` |
| 2 | Сгенерировать ключ | `ssh-keygen -t ed25519 -C "email" -f ~/.ssh/id_ed25519 -N ""` |
| 3 | Добавить ключ в агент | `eval "$(ssh-agent -s)"` и `ssh-add --apple-use-keychain ~/.ssh/id_ed25519` |
| 4 | Добавить ключ в GitHub | Скопировать `~/.ssh/id_ed25519.pub` → https://github.com/settings/keys → New SSH key |
| 5 | Добавить GitHub в known_hosts | `ssh-keyscan -t ed25519,rsa github.com >> ~/.ssh/known_hosts` |
| 6 | Проверить | `ssh -T git@github.com` |
| 7 | Remote на SSH | `git remote set-url origin git@github.com:akhromovRT/pass24-knowlege-base.git` |
| 8 | Push | `git push origin main` |

---

## Частые проблемы

**«Host key verification failed»**  
— Выполните шаг 5 (ssh-keyscan в known_hosts) и при запросе введите `yes`.

**«Permission denied (publickey)»**  
— Публичный ключ не добавлен в GitHub или добавлен не тот ключ. Проверьте шаг 4 и при необходимости шаг 2.

**«Could not open a connection to your authentication agent»**  
— Запустите агент: `eval "$(ssh-agent -s)"`, затем снова `ssh-add ~/.ssh/id_ed25519`.

**Remote по-прежнему HTTPS**  
— Выполните шаг 7: `git remote set-url origin git@github.com:akhromovRT/pass24-knowlege-base.git`.

---

*Документ создан для настройки SSH в проекте базы знаний PASS24. Репозиторий: https://github.com/akhromovRT/pass24-knowlege-base*
