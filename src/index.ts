import { Context, Schema, Session, Database, h } from 'koishi' // 保持你添加的 Database 导入

export const name = 'giftcard'

// 添加数据库依赖
export const using = ['database']

// 定义配置接口
export interface Config {
  targetGroups: string[];
  adminIds: string[];
}

// 定义配置 Schema
export const Config: Schema<Config> = Schema.object({
  targetGroups: Schema.array(Schema.string())
    .description('启用礼品卡功能的群号列表。')
    .default([]),
  adminIds: Schema.array(Schema.string())
    .description('可以执行管理操作的管理员 QQ 号列表。')
    .default([]),
})

// 扩展 Koishi 的数据库表 (使用 'koishi' 路径)
declare module 'koishi' {
  interface Tables {
    giftcard_cards: GiftCardEntry
    giftcard_invites: GiftCardInviteLog
  }
}

// 礼品卡表条目的接口
export interface GiftCardEntry {
  id: number          // 主键
  card_code: string   // 实际的礼品卡代码
  assigned_to_user_id: string | null // 分配给的用户的 QQ ID (如果未分配则为 null)
  assigned_timestamp: Date | null   // 分配时间
  added_by_admin_id: string // 添加该卡的管理员 ID
  add_timestamp: Date         // 卡添加时间
}

// 邀请记录表条目的接口
export interface GiftCardInviteLog {
  id: number          // 主键
  inviter_id: string  // 邀请人的 QQ ID
  invited_id: string  // 被邀请用户的 QQ ID
  group_id: string    // 邀请发生的群组 ID
  timestamp: Date     // 奖励发放时间
  inviter_card_id: number | null // 发给邀请人的卡的 ID
  invited_card_id: number | null // 发给被邀请人的卡的 ID
}

export function apply(ctx: Context, config: Config) {
  // --- 数据库表定义 ---
  ctx.model.extend('giftcard_cards', {
    id: 'unsigned', // 自增主键
    card_code: { type: 'string', length: 255 },
    assigned_to_user_id: { type: 'string', length: 50, nullable: true },
    assigned_timestamp: { type: 'timestamp', nullable: true },
    added_by_admin_id: { type: 'string', length: 50 },
    add_timestamp: 'timestamp',
  }, {
    primary: 'id',
    unique: ['card_code'], // 确保卡码唯一
  });

  ctx.model.extend('giftcard_invites', {
    id: 'unsigned',
    inviter_id: { type: 'string', length: 50 },
    invited_id: { type: 'string', length: 50 },
    group_id: { type: 'string', length: 50 },
    timestamp: 'timestamp',
    inviter_card_id: { type: 'unsigned', nullable: true }, // 外键关系 (可选，但推荐)
    invited_card_id: { type: 'unsigned', nullable: true }, // 外键关系 (可选，但推荐)
  }, {
    primary: 'id',
    // 防止在同一群组中为同一次邀请重复发放奖励
    unique: ['inviter_id', 'invited_id', 'group_id'],
  });

  // --- 辅助函数 ---
  const isAdmin = (session: Session): boolean => {
    // 确保 session 和 userId 存在
    return session?.userId ? config.adminIds.includes(session.userId) : false;
  }

  // --- 群成员增加事件监听器 (已有手动群组检查) ---
  ctx.on('guild-member-added', async (session) => {
    // 检查事件是否发生在目标群组
    if (!session.guildId || !config.targetGroups.includes(session.guildId)) {
      return; // 不在目标群组，直接返回
    }

    const inviterId = session.operatorId;
    const invitedId = session.userId;

    // 检查是否是有效邀请
    if (!inviterId || inviterId === invitedId) {
      ctx.logger('giftcard').debug(`用户 ${invitedId} 加入群 ${session.guildId} 时没有明确的邀请人 (操作者ID: ${inviterId})。`);
      return;
    }

    ctx.logger('giftcard').info(`用户 ${invitedId} 加入群 ${session.guildId}, 由 ${inviterId} 邀请。正在检查奖励...`);

    try {
      // 检查是否已奖励过
      const existingInvite = await ctx.database.get('giftcard_invites', {
        inviter_id: inviterId,
        invited_id: invitedId,
        group_id: session.guildId,
      });

      if (existingInvite.length > 0) {
        ctx.logger('giftcard').warn(`邀请奖励已发放给 ${inviterId} -> ${invitedId} 在群 ${session.guildId}。`);
        return;
      }

      // 查找可用卡
      const availableCards = await ctx.database.get('giftcard_cards', {
        assigned_to_user_id: null,
      }, { limit: 2 });

      if (availableCards.length < 2) {
        ctx.logger('giftcard').warn(`没有足够的未分配礼品卡来奖励群 ${session.guildId} 中的邀请。需要 2 张，找到 ${availableCards.length} 张。`);
        // 通知管理员缺卡
        for (const adminId of config.adminIds) {
          try {
            await session.bot.sendPrivateMessage(adminId, `礼品卡插件警告：群 ${session.guildId} 中有新成员 (${invitedId}) 加入，邀请者为 (${inviterId})，但系统没有足够的未分配礼品卡。请及时补充。`);
          } catch (e) {
            ctx.logger('giftcard').warn(`发送缺卡警告给管理员 ${adminId} 失败: ${e}`)
          }
        }
        return;
      }

      const cardForInviter = availableCards[0];
      const cardForInvited = availableCards[1];
      const now = new Date();

      await ctx.database.withTransaction(async (tx) => {
        // 1. 使用 upsert 批量更新两张卡的状态
        // (此部分与上次修改相同)
        await tx.upsert('giftcard_cards', [
          { // 更新邀请者的卡
            card_code: cardForInviter.card_code, // 匹配条件：唯一卡号
            assigned_to_user_id: inviterId,      // 要更新/设置的字段
            assigned_timestamp: now             // 要更新/设置的字段
          },
          { // 更新被邀请者的卡
            card_code: cardForInvited.card_code, // 匹配条件：唯一卡号
            assigned_to_user_id: invitedId,       // 要更新/设置的字段
            assigned_timestamp: now              // 要更新/设置的字段
          }
        ],'card_code');

        // 2. 使用 upsert 插入邀请记录
        // 由于之前的检查保证了 (inviter_id, invited_id, group_id) 组合不存在，
        // 这条 upsert 实际上会执行 INSERT 操作。
        // 我们需要提供所有要插入的字段，数据库会自动处理自增的 'id'。
        await tx.upsert('giftcard_invites', [{ // 将要插入的数据对象放在数组中
          inviter_id: inviterId,
          invited_id: invitedId,
          group_id: session.guildId,
          timestamp: now,
          inviter_card_id: cardForInviter.id, // 卡片 ID 仍然需要
          invited_card_id: cardForInvited.id, // 卡片 ID 仍然需要
        }]);
      })
      ctx.logger('giftcard').info(`成功将卡 ${cardForInviter.card_code} 分配给邀请人 ${inviterId}，将卡 ${cardForInvited.card_code} 分配给被邀请人 ${invitedId}。`);

      // --- 私聊通知用户 ---
      try {
        await session.bot.sendPrivateMessage(inviterId, `感谢您邀请新成员加入群 ${session.guildId}！您已获得一个礼品卡： ${cardForInviter.card_code}`);
      } catch (error) {
        ctx.logger('giftcard').warn(`发送礼品卡私信给邀请人 ${inviterId} 失败: ${error}`);
      }

      try {
        await session.bot.sendPrivateMessage(invitedId, `欢迎加入群 ${session.guildId}！您已被 ${inviterId} 邀请，并获得一个礼品卡： ${cardForInvited.card_code}`);
      } catch (error) {
        ctx.logger('giftcard').warn(`发送礼品卡私信给被邀请用户 ${invitedId} 失败: ${error}`);
      }

    } catch (error) {
      ctx.logger('giftcard').error(`处理群 ${session.guildId} 的 guild-member-added 事件时出错: ${error}`);
    }
  });

  // --- 命令定义 (移除 .channel 中间件) ---
  const cmd = ctx.command('giftcard', '礼品卡管理与查询');

  // --- 子命令: .add (仅管理员，无需群组检查) ---
  cmd.subcommand('.add <cards:text>', '添加新的礼品卡 (限管理员)')
    .option('silent', '-s 不报告已存在或失败的卡')
    .action(async ({ session, options }, cardsText) => {
      if (!session || !session.userId) return;
      if (!isAdmin(session)) return '抱歉，只有管理员才能添加礼品卡。';

      if (!cardsText) {
        return '请提供要添加的礼品卡代码，以空格分隔。\n用法: giftcard.add <卡码1> [卡码2] ...';
      }

      // 1. 解析并去重输入的卡码
      const inputCardCodes = [...new Set(cardsText.trim().split(/\s+/).filter(code => code.length > 0))];
      if (inputCardCodes.length === 0) {
        return '未提供有效的礼品卡代码。';
      }

      const now = new Date();
      const adminId = session.userId;
      const successfullyAddedCodes: string[] = []; // 用于记录确认新添加的
      const alreadyExistedCodes: string[] = [];
      let errorMessage = '';

      try {
        // 2. 检查哪些卡码已经存在 (为了精确报告)
        const existingCards = await ctx.database.get('giftcard_cards',
          { card_code: { $in: inputCardCodes } },
          ['card_code']
        );
        const existingCodeSet = new Set(existingCards.map(card => card.card_code));

        // 3. 筛选出需要尝试 upsert 的新卡码
        // (实际上，upsert 会处理已存在的，但我们筛选是为了能准确报告“新增”)
        const codesToAttemptUpsert = inputCardCodes.filter(code => !existingCodeSet.has(code));
        inputCardCodes.forEach(code => {
          if (existingCodeSet.has(code)) {
            alreadyExistedCodes.push(code);
          }
        });

        // 4. 只对新卡码准备 upsert 数据 (避免不必要的更新操作)
        if (codesToAttemptUpsert.length > 0) {
          const newCardEntriesForUpsert = codesToAttemptUpsert.map(code => ({
            card_code: code, // 必须提供唯一键
            // 提供插入时需要设置的字段和值
            assigned_to_user_id: null,
            assigned_timestamp: null,
            added_by_admin_id: adminId,
            add_timestamp: now,
          }));

          // 5. 执行批量 upsert
          // 这里使用直接传递数组的方式，更符合批量数据的场景
          await ctx.database.upsert('giftcard_cards', newCardEntriesForUpsert);

          // 假设 upsert 成功执行并没有抛出错误，那么这些卡就是成功添加的
          successfullyAddedCodes.push(...codesToAttemptUpsert);
        }

      } catch (error) {
        errorMessage = error instanceof Error ? error.message : String(error);
        ctx.logger('giftcard').error(`Upsert 礼品卡失败: ${errorMessage}`);
        errorMessage = `数据库操作时发生错误: ${errorMessage.substring(0, 100)}`; // 截断错误信息
        // 如果出错，需要告知用户哪些卡可能未添加成功
      }

      // 6. 构建反馈报告 (与 create 版本类似)
      let report = `礼品卡处理完成：\n成功添加 ${successfullyAddedCodes.length} 个新卡。`;
      if (successfullyAddedCodes.length > 0) {
        report += `\n新卡码: ${successfullyAddedCodes.join(', ')}`;
      }

      if (!options.silent) {
        if (alreadyExistedCodes.length > 0) {
          report += `\n\n已存在 ${alreadyExistedCodes.length} 个（已跳过）：\n`;
          report += alreadyExistedCodes.map(code => `- ${h.escape(code)}`).join('\n');
        }
        if (errorMessage) {
          report += `\n\n处理过程中发生错误：${h.escape(errorMessage)}`;
          // 找出输入中既没成功也没报告已存在的，视为可能失败
          const possiblyFailed = inputCardCodes.filter(c => !successfullyAddedCodes.includes(c) && !alreadyExistedCodes.includes(c));
          if (possiblyFailed.length > 0) {
            report += `\n以下卡可能未能处理：${possiblyFailed.join(', ')}`;
          }
        }
      } else if (alreadyExistedCodes.length > 0 || errorMessage) {
        report += `\n有 ${alreadyExistedCodes.length} 个卡已存在或处理失败（详情已隐藏）。`;
      }

      return report;
    });

  // --- 子命令: .info (仅管理员，无需群组检查) ---
  cmd.subcommand('.info', '列出所有礼品卡状态 (限管理员)')
    .action(async ({ session }) => {
      if (!session || !session.userId) return;
      if (!isAdmin(session)) return '抱歉，只有管理员才能查看礼品卡列表。';

      try {
        const allCards = await ctx.database.get('giftcard_cards', {});
        if (allCards.length === 0) {
          return '系统中还没有任何礼品卡。';
        }

        const assignedCards = allCards.filter(card => card.assigned_to_user_id);
        const unassignedCards = allCards.filter(card => !card.assigned_to_user_id);

        let report = `礼品卡状态报告 (总数: ${allCards.length} | 未分配: ${unassignedCards.length} | 已分配: ${assignedCards.length})：\n\n`;
        report += `--- 未分配 (${unassignedCards.length} 张) ---\n`;
        if (unassignedCards.length > 0) {
          const displayLimit = 30; // 限制显示数量防止消息过长
          report += unassignedCards.slice(0, displayLimit).map(card => `- ${h.escape(card.card_code)} (由 ${card.added_by_admin_id} 于 ${card.add_timestamp.toLocaleString()} 添加)`).join('\n');
          if (unassignedCards.length > displayLimit) {
            report += `\n... (还有 ${unassignedCards.length - displayLimit} 张未显示)`;
          }
        } else {
          report += '(无)';
        }

        report += `\n\n--- 已分配 (${assignedCards.length} 张) ---\n`;
        if (assignedCards.length > 0) {
          report += assignedCards.map(card => `- ${h.escape(card.card_code)} -> 用户 ${card.assigned_to_user_id} (于 ${card.assigned_timestamp?.toLocaleString() ?? '未知时间'})`).join('\n');
        } else {
          report += '(无)';
        }

        // Koishi v4 通常能处理较长消息分片，直接返回即可
        return report;

      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        ctx.logger('giftcard').error(`获取礼品卡信息失败: ${errorMessage}`);
        return '查询礼品卡信息时发生错误，请查看日志。';
      }
    });

  // --- 子命令: .me (仅限私聊，无需群组检查) ---
  cmd.subcommand('.me', '私聊查询自己获得的礼品卡')
    .action(async ({ session }) => {
      if (!session || !session.userId) return;

      // 强制私聊环境
      if (session?.event?.channel?.type !== 1) { // ChannelType.DIRECT = 1
        await session.send('为了您的礼品卡安全，请通过私聊使用此命令查询。');
        return;
      }

      try {
        const myCards = await ctx.database.get('giftcard_cards', {
          assigned_to_user_id: session.userId,
        });

        if (myCards.length === 0) {
          return '您目前还没有通过邀请获得任何礼品卡。';
        }

        const cardList = myCards.map(card => `- ${h.escape(card.card_code)} (获得时间: ${card.assigned_timestamp?.toLocaleString() ?? '未知'})`).join('\n');
        return `您拥有的礼品卡列表：\n${cardList}`;

      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        ctx.logger('giftcard').error(`为用户 ${session.userId} 获取礼品卡失败: ${errorMessage}`);
        return '查询您的礼品卡时遇到问题，请稍后再试或联系管理员。';
      }
    });

  // --- 默认动作: 列出邀请的人 (添加手动群组检查) ---
  cmd.action(async ({ session }) => {
    // 必须在群聊中执行
    if (!session || !session.userId || !session.guildId || session?.event?.channel?.type === 1) {
      if (session?.event?.channel?.type === 1) {
        return '此命令用于在群内查看您的邀请记录。\n要查看您的礼品卡, 请私聊我并发送 "giftcard.me"。';
      }
      return; // 非群聊环境直接忽略
    }

    // * * * 手动检查是否在目标群组 * * *
    if (!config.targetGroups.includes(session.guildId)) {
      // return; // 静默返回，不在配置群组则无响应
      return `抱歉，礼品卡邀请功能未在当前群组 (${session.guildId}) 启用。`; // 明确提示
    }

    // 查询邀请记录
    try {
      const successfulInvites = await ctx.database.get('giftcard_invites', {
        inviter_id: session.userId,
        group_id: session.guildId, // 只查询当前群的
      });

      if (successfulInvites.length === 0) {
        return `您在本群 (${session.guildId}) 还没有成功邀请过任何获得礼品卡的成员。`;
      }

      // 尝试获取被邀请者昵称
      let invitedListStr = "";
      try {
        const invitedIds = successfulInvites.map(invite => invite.invited_id);
        // 批量获取成员信息可能更高效，但逐个获取并处理错误更稳健
        const memberDetailsPromises = invitedIds.map(id =>
          session.bot.getGuildMember(session.guildId, id)
            .catch((err) => { // 捕获获取单个成员信息失败
              ctx.logger('giftcard').warn(`无法获取群 ${session.guildId} 成员 ${id} 的信息: ${err}`);
              // 返回一个包含ID的回退对象
              return { user: { id: id, name: id, nick: id }, nick: id };
            })
        );
        const memberDetails = await Promise.all(memberDetailsPromises);

        invitedListStr = memberDetails.map((memberInfo, index) => {
          const invite = successfulInvites[index];
          // 优先用群昵称，其次用户昵称/名，最后用ID
          const name = memberInfo?.nick || memberInfo?.user?.name || memberInfo?.user?.nick || invite.invited_id;
          return `- ${h.escape(name)} (ID: ${invite.invited_id}) 于 ${invite.timestamp.toLocaleString()}`;
        }).join('\n');

      } catch (nameError) {
        ctx.logger('giftcard').warn(`获取群 ${session.guildId} 邀请列表成员名称时出错: ${nameError}`);
        // 出错时回退到只显示 ID
        invitedListStr = successfulInvites.map(invite => `- 用户 ID: ${invite.invited_id} (于 ${invite.timestamp.toLocaleString()})`).join('\n');
      }

      return `您在本群 (${session.guildId}) 成功邀请并获得奖励的成员列表：\n${invitedListStr}`;

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      ctx.logger('giftcard').error(`为用户 ${session.userId} 在群 ${session.guildId} 获取邀请记录失败: ${errorMessage}`);
      return '查询邀请记录时发生错误，请重试或联系管理员。';
    }
  });
}