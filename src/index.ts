import { createDecipheriv } from 'crypto';
import { Context, Schema, Session, h } from 'koishi'

export const name = 'giftcard'

export const reusable = true

// 添加数据库依赖
export const inject = ['database']

// 定义配置接口
export interface Config {
  commandname: string;
  targetGroups: string[];
  adminIds: string[];
  codeperpage: number;
  cardsPerInviter: number;
  rewardInvitedUser: boolean;
  enableWelcomeMessage: boolean;
  welcomeMessageFormat: string;
}

// 定义配置 Schema
export const Config: Schema<Config> = Schema.object({
  commandname: Schema.string().default('giftcard').description('注册的指令'),
  targetGroups: Schema.array(Schema.string())
    .description('启用礼品卡和欢迎语功能的群号列表。')
    .default([]),
  adminIds: Schema.array(Schema.string())
    .description('可以执行管理操作的管理员 QQ 号列表。')
    .default([]),
  cardsPerInviter: Schema.natural()
    .description('每次成功邀请，邀请者获得的礼品卡数量。')
    .default(1),
  codeperpage: Schema.natural()
    .description('每页显示的礼品卡数量。')
    .default(10),
  rewardInvitedUser: Schema.boolean()
    .description('是否给被邀请的新成员发放礼品卡 (发放1张)。')
    .default(true),
  enableWelcomeMessage: Schema.boolean()
    .description('是否在该群发送入群欢迎语。')
    .default(false), // 默认关闭欢迎语
  welcomeMessageFormat: Schema.string()
    .description('欢迎语格式。可用占位符：{user} (艾特新成员), {inviter} (艾特邀请人，如果存在), {groupName} (群名称)')
    .default('欢迎新成员 {user} 加入本群！'), // 默认欢迎语
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
  id: number;
  card_code: string;
  assigned_to_user_id: string | null;
  assigned_timestamp: Date | null;
  added_by_admin_id: string;
  add_timestamp: Date;
  left: number;
  isreusable: boolean;
}

// 邀请记录表条目的接口
export interface GiftCardInviteLog {
  id: number;
  inviter_id: string | null; // 允许邀请人为 null (直接加入)
  invited_id: string;
  group_id: string;
  timestamp: Date;
  inviter_card_id: number | null;
  invited_card_id: number | null;
}

export function apply(ctx: Context, config: Config) {
  // --- 数据库表定义 ---
  ctx.model.extend('giftcard_cards', {
    id: 'unsigned',
    card_code: { type: 'string', length: 255 },
    assigned_to_user_id: { type: 'string', length: 50, nullable: true },
    assigned_timestamp: { type: 'timestamp', nullable: true },
    added_by_admin_id: { type: 'string', length: 50 },
    add_timestamp: 'timestamp',
    left: 'unsigned',
    isreusable: 'boolean',
  }, {
    primary: 'id',
    unique: ['card_code'],
  });

  ctx.model.extend('giftcard_invites', {
    id: 'unsigned',
    inviter_id: { type: 'string', length: 50, nullable: true }, // 允许为 null
    invited_id: { type: 'string', length: 50 },
    group_id: { type: 'string', length: 50 },
    timestamp: 'timestamp',
    inviter_card_id: { type: 'unsigned', nullable: true },
    invited_card_id: { type: 'unsigned', nullable: true },
  }, {
    primary: 'id',
  });

  const isAdmin = (session: Session): boolean => {
    return session?.userId ? config.adminIds.includes(session.userId) : false;
  }

  // --- 群成员增加事件监听器 ---
  ctx.on('guild-member-added', async (session) => {
    // 1. 检查是否在目标群组
    if (!session.guildId || !config.targetGroups.includes(session.guildId)) {
      return;
    }

    const invitedId = session.userId;
    const inviterId = session.operatorId; // 可能为 null 或等于 invitedId

    // 2. 发送欢迎语 (如果开启)
    if (config.enableWelcomeMessage && config.welcomeMessageFormat) {
      try {
        let welcomeMsg = config.welcomeMessageFormat;
        // 替换占位符
        welcomeMsg = welcomeMsg.replace(/\{user\}/g, h.at(invitedId).toString()); // 艾特新成员
        if (inviterId && inviterId !== invitedId) {
          welcomeMsg = welcomeMsg.replace(/\{inviter\}/g, h.at(inviterId).toString()); // 艾特邀请人
        } else {
          welcomeMsg = welcomeMsg.replace(/\{inviter\}/g, ''); // 如果没有邀请人，移除占位符
        }
        const guildName = session.event?.guild?.name || session.guildId; // 尝试获取群名
        welcomeMsg = welcomeMsg.replace(/\{groupName\}/g, h.escape(guildName)); // 替换群名

        await session.send(welcomeMsg); // 在群里发送欢迎语
      } catch (e) {
        //ctx.logger('giftcard').warn(`发送欢迎语到群 ${session.guildId} 失败: ${e}`);
      }
    }

    // 3. 检查是否是直接加入 (非邀请)
    const isDirectJoin = !inviterId || inviterId === invitedId;
    if (isDirectJoin) {
      //ctx.logger('giftcard').info(`用户 ${invitedId} 直接加入群 ${session.guildId} (无有效邀请人)。`);
      return; // 直接加入不参与奖励逻辑
    }
    try {
      const previousJoins = await ctx.database.get('giftcard_invites', {
        invited_id: invitedId, // 只关心被邀请者
        group_id: session.guildId, // 和当前群组
      });

      if (previousJoins.length > 0) {
        //ctx.logger('giftcard').warn(`用户 ${invitedId} 已被记录过加入群 ${session.guildId}，本次邀请 (${inviterId}) 不再发放奖励。`);
        return; // 已处理过此用户，不再发放奖励
      }
    } catch (error) {
      //ctx.logger('giftcard').error(`查询用户 ${invitedId} 在群 ${session.guildId} 的历史记录失败: ${error}`);
      return; // 查询失败，为安全起见不发放奖励
    }


    const cardsNeededForInviter = Math.max(0, config.cardsPerInviter ?? 1);
    const cardsNeededForInvited = config.rewardInvitedUser ? 1 : 0;
    const totalCardsNeeded = cardsNeededForInviter + cardsNeededForInvited;

    if (totalCardsNeeded === 0) {
      //ctx.logger('giftcard').info(`用户 ${invitedId} 由 ${inviterId} 首次邀请加入群 ${session.guildId}，但根据配置无需发放礼品卡。`);
      await ctx.database.upsert('giftcard_invites', [{
        inviter_id: inviterId,
        invited_id: invitedId,
        group_id: session.guildId,
        timestamp: new Date(),
        inviter_card_id: null,
        invited_card_id: null,
      }]);
      return;
    }

    //ctx.logger('giftcard').info(`用户 ${invitedId} 由 ${inviterId} 首次邀请加入群 ${session.guildId}。检查奖励 (需要 ${cardsNeededForInviter} for inviter, ${cardsNeededForInvited} for invited, total ${totalCardsNeeded})...`);

    try {
      const availableCards = await ctx.database.get('giftcard_cards', {
        assigned_to_user_id: null,
      });
      //累加法记录每个isreusable为true的礼品卡left与isreusable为false的礼品卡总数之和
      const totalAvailableCards = availableCards.reduce((acc, card) => acc + (card.isreusable ? card.left : 1), 0);

      if (totalAvailableCards < totalCardsNeeded) {
        //ctx.logger('giftcard').warn(`没有足够的未分配礼品卡来奖励群 ${session.guildId} 中的邀请。需要 ${totalCardsNeeded} 张，找到 ${totalAvailableCards} 张。`);
        for (const adminId of config.adminIds) {
          try {
            await session.bot.sendPrivateMessage(adminId, `礼品卡插件警告：群 ${session.guildId} 中有新成员 (${invitedId}) 加入，邀请者为 (${inviterId})，但系统没有足够的未分配礼品卡来完成奖励 (需要 ${totalCardsNeeded} 张)。请及时补充。`);
          } catch (e) {
            //ctx.logger('giftcard').warn(`发送缺卡警告给管理员 ${adminId} 失败: ${e}`)
          }
        }
        return;
      }

      const now = new Date();
      const cardsForInviter: GiftCardEntry[] = [];
      let cardForInvited: GiftCardEntry | null = null;

      
      // if (cardsNeededForInviter > 0) {
      //   cardsForInviter.push(...availableCards.slice(0, cardsNeededForInviter));
      // }
      // if (cardsNeededForInvited > 0) {
      //   cardForInvited = availableCards[cardsNeededForInviter];
      // }

      // const cardsToUpdateInDB: Partial<GiftCardEntry>[] = [];
      // cardsForInviter.forEach(card => {
      //   cardsToUpdateInDB.push({
      //     card_code: card.card_code,
      //     assigned_to_user_id: inviterId,
      //     assigned_timestamp: now
      //   });
      // });
      // if (cardForInvited) {
      //   cardsToUpdateInDB.push({
      //     card_code: cardForInvited.card_code,
      //     assigned_to_user_id: invitedId,
      //     assigned_timestamp: now
      //   });
      // }

      // await ctx.database.withTransaction(async (tx) => {
      //   if (cardsToUpdateInDB.length > 0) {
      //     await tx.upsert('giftcard_cards', cardsToUpdateInDB, 'card_code');
      //   }
      //   await tx.upsert('giftcard_invites', [{
      //     inviter_id: inviterId, // 记录本次的邀请人
      //     invited_id: invitedId,
      //     group_id: session.guildId,
      //     timestamp: now,
      //     inviter_card_id: cardsForInviter[0]?.id ?? null,
      //     invited_card_id: cardForInvited?.id ?? null,
      //   }]);
      // })
      // 修改为随机分配，reusable的卡先判断还有没有（left是不是0），reusable的卡减left不写邀请人，非reusable的卡直接写邀请人不减left
      if (cardsNeededForInviter > 0) {
        const reusableCards = availableCards.filter(card => card.isreusable);
        const nonReusableCards = availableCards.filter(card => !card.isreusable);
        const reusableCardsNeeded = Math.min(reusableCards.length, cardsNeededForInviter);
        const nonReusableCardsNeeded = cardsNeededForInviter - reusableCardsNeeded;
        cardsForInviter.push(...reusableCards.slice(0, reusableCardsNeeded));
        cardsForInviter.push(...nonReusableCards.slice(0, nonReusableCardsNeeded));
        //按reusable分别处理卡
        for (const card of cardsForInviter) {
          if (card.isreusable) {
            await ctx.database.set('giftcard_cards', { card_code: card.card_code }, { left: card.left - 1 });
          } else {
            await ctx.database.set('giftcard_cards', { card_code: card.card_code }, { assigned_to_user_id: inviterId, assigned_timestamp: now });
          }
        }
      }
      if (cardsNeededForInvited > 0) {
        const card = availableCards[cardsNeededForInviter];
        if (card.isreusable) {
          await ctx.database.set('giftcard_cards', { card_code: card.card_code }, { left: card.left - 1 });
        } else {
          await ctx.database.set('giftcard_cards', { card_code: card.card_code }, { assigned_to_user_id: invitedId, assigned_timestamp: now });
        }
      }

      // --- 记录日志和通知用户 (与之前逻辑类似) ---
      const inviterCardCodes = cardsForInviter.map(c => c.card_code).join(', ');
      const invitedCardCode = cardForInvited?.card_code;
      //ctx.logger('giftcard').info(`成功分配礼品卡：邀请者 ${inviterId} 获得 ${cardsNeededForInviter} 张 (${inviterCardCodes || '无'})，被邀请者 ${invitedId} 获得 ${cardsNeededForInvited} 张 (${invitedCardCode || '无'})。`);

      if (cardsNeededForInviter > 0) {
        try {
          let inviterMessage = `感谢您邀请新成员加入群 ${session.guildId}！`;
          inviterMessage += cardsNeededForInviter === 1
            ? `您已获得 ${cardsNeededForInviter} 个礼品卡： ${inviterCardCodes}`
            : `您已获得 ${cardsNeededForInviter} 个礼品卡：\n${cardsForInviter.map(c => `- ${c.card_code}`).join('\n')}`;
          await session.bot.sendPrivateMessage(inviterId, inviterMessage);
        } catch (error) {
          //ctx.logger('giftcard').warn(`发送礼品卡私信给邀请人 ${inviterId} 失败: ${error}`);
        }
      }

      if (cardForInvited) {
        try {
          await session.bot.sendPrivateMessage(invitedId, `欢迎加入群 ${session.guildId}！您已被 ${inviterId} 邀请，并获得一个礼品卡： ${cardForInvited.card_code}`);
        } catch (error) {
          //ctx.logger('giftcard').warn(`发送礼品卡私信给被邀请用户 ${invitedId} 失败: ${error}`);
        }
      }

    } catch (error) { // 这个 catch 现在主要捕获分配卡片和数据库操作的错误
      const errorMessage = error instanceof Error ? error.message : String(error);
      //ctx.logger('giftcard').error(`处理群 ${session.guildId} 的 guild-member-added 奖励逻辑时出错: ${errorMessage}`);
    }
  });

  const cmd = ctx.command(config.commandname, '礼品卡管理与查询');

  cmd.subcommand('.add <cards:text>', '添加新的礼品卡 (限管理员)')
    .option('silent', '-s 不报告已存在或失败的卡')
    .action(async ({ session, options }, cardsText) => {

      if (!session || !session.userId) return;
      if (!isAdmin(session)) return '抱歉，只有管理员才能添加礼品卡。';
      if (!cardsText) {
        return '请提供要添加的礼品卡代码，以空格分隔。\n用法: giftcard.add <卡码1> [卡码2] ...';
      }

      const inputCardCodes = [...new Set(cardsText.trim().split(/\s+/).filter(code => code.length > 0))];
      if (inputCardCodes.length === 0) {
        return '未提供有效的礼品卡代码。';
      }

      const now = new Date();
      const adminId = session.userId;
      const successfullyAddedCodes: string[] = [];
      const alreadyExistedCodes: string[] = [];
      let errorMessage = '';

      try {
        const existingCards = await ctx.database.get('giftcard_cards',
          { card_code: { $in: inputCardCodes } },
          ['card_code']
        );
        const existingCodeSet = new Set(existingCards.map(card => card.card_code));

        const codesToAttemptUpsert = inputCardCodes.filter(code => !existingCodeSet.has(code));
        inputCardCodes.forEach(code => {
          if (existingCodeSet.has(code)) {
            alreadyExistedCodes.push(code);
          }
        });

        if (codesToAttemptUpsert.length > 0) {
          const newCardEntriesForUpsert = codesToAttemptUpsert.map(code => ({
            card_code: code,
            assigned_to_user_id: null,
            assigned_timestamp: null,
            added_by_admin_id: adminId,
            add_timestamp: now,
            isreusable: false
          }));

          await ctx.database.upsert('giftcard_cards', newCardEntriesForUpsert);
          successfullyAddedCodes.push(...codesToAttemptUpsert);
        }

      } catch (error) {
        errorMessage = error instanceof Error ? error.message : String(error);
        ctx.logger('giftcard').error(`Upsert 礼品卡失败: ${errorMessage}`);
        errorMessage = `数据库操作时发生错误: ${errorMessage.substring(0, 100)}`;
      }

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

  // --- 子命令: .info (不变) ---
  cmd.subcommand('.info', '查看礼品卡状态 (限管理员) <page:number>')
    .option('card', '-c [card] 查询指定卡码的详细信息')
    .action(async ({ session, options }, page) => {
      
      if (!session || !session.userId) return;
      if (!isAdmin(session)) return '抱歉，只有管理员才能查看礼品卡状态。';
      if (!options.card) {
        try {
          const allCards = await ctx.database.get('giftcard_cards', {});
          if (allCards.length === 0) {
            return '系统中还没有任何礼品卡。';
          }

          const assignedCards = allCards.filter(card => card.assigned_to_user_id);
          const unassignedCards = allCards.filter(card => !card.assigned_to_user_id);

          let report = `礼品卡状态报告 (总数: ${allCards.length} | 未分配: ${unassignedCards.length} | 已分配: ${assignedCards.length})：\n第${page}页\n`;
          report += `--- 未分配 (${unassignedCards.length} 张) ---\n`;
          const pages = parseInt(page ?? '1');
          const pageSize = config.codeperpage;
          const startIndex = (pages - 1) * pageSize;
          const endIndex = startIndex + pageSize;
          if (unassignedCards.length >= startIndex) {
            //分页列出第page页，如果页数不够就显示无匹配的邀请码，isreusable为true则显示可重复使用且显示剩余数量
            const paginatedUnassignedCards = unassignedCards.slice(startIndex, endIndex);
            paginatedUnassignedCards.forEach(card => {
              report += `- ${h.escape(card.card_code)} (添加者: ${card.added_by_admin_id})${card.isreusable ? ' (可重复使用)' : ''}${card.isreusable ? ` (剩余: ${card.left})` : ''}\n`;
            }
            )
          } else {
            report += '(无匹配条件的筛选)';
          }

          report += `\n\n--- 已分配 (${assignedCards.length} 张) ---\n`;
          if (assignedCards.length >= startIndex) {
            //分页列出第page页，如果页数不够就显示"无匹配的邀请码"
            const paginatedAssignedCards = assignedCards.slice(startIndex, endIndex);
            report += paginatedAssignedCards.map(card => `- ${h.escape(card.card_code)} (持有者: ${card.assigned_to_user_id})`).join('\n');
          } else {
            report += '(无匹配条件的筛选)';
          }

          return report;

        } catch (error) {
          const errorMessage = error instanceof Error ? error.message : String(error);
          ctx.logger('giftcard').error(`获取礼品卡信息失败: ${errorMessage}`);
          return '查询礼品卡信息时发生错误，请查看日志。';
        }
      } else {
        try {
          const card = await ctx.database.get('giftcard_cards', { card_code: options.card });
          if (card.length === 0) {
            return `礼品卡 ${h.escape(options.card)} 不存在。`;
          } else {
            const cardInfo = card[0];
            return `礼品卡 ${h.escape(options.card)} 的信息：\n- 持有者: ${cardInfo.assigned_to_user_id ?? '未分配'}\n- 添加者: ${cardInfo.added_by_admin_id}\n- 添加时间: ${cardInfo.add_timestamp?.toLocaleString() ?? '未知'}\n${cardInfo.isreusable ? '- 可重复使用' : '- 不可重复使用'}${cardInfo.isreusable ? ` (剩余: ${cardInfo.left})` : ''}\n- 分配时间: ${cardInfo.assigned_timestamp?.toLocaleString() ?? '未分配'}`;
          }
        } catch (error) {
          const errorMessage = error instanceof Error ? error.message : String(error);
          ctx.logger('giftcard').error(`获取礼品卡信息失败: ${errorMessage}`);
          return '查询礼品卡信息时发生错误，请查看日志。';
        }
      }
    });

  // --- 子命令: .me (不变) ---
  cmd.subcommand('.me', '私聊查询自己获得的礼品卡')
    .action(async ({ session }) => {
      if (!session || !session.userId) return;

      if (session?.event?.channel?.type !== 1) {
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

  // --- 默认动作: 列出邀请的人 (不变) ---
  cmd.action(async ({ session }) => {
    if (!session || !session.userId || !session.guildId || session?.event?.channel?.type === 1) {
      if (session?.event?.channel?.type === 1) {
        return '此命令用于在群内查看您的邀请记录。\n要查看您的礼品卡, 请私聊我并发送 "giftcard.me"。';
      }
      return;
    }

    if (!config.targetGroups.includes(session.guildId)) {
      return `抱歉，礼品卡邀请功能未在当前群组 (${session.guildId}) 启用。`;
    }

    try {
      const successfulInvites = await ctx.database.get('giftcard_invites', {
        inviter_id: session.userId, // 仍然只查当前用户作为邀请人的记录
        group_id: session.guildId,
      });

      if (successfulInvites.length === 0) {
        return `您在本群 (${session.guildId}) 还没有成功邀请过任何获得礼品卡的成员。`;
      }

      let invitedListStr = "";
      try {
        const invitedIds = successfulInvites.map(invite => invite.invited_id);
        const memberDetailsPromises = invitedIds.map(id =>
          session.bot.getGuildMember(session.guildId, id)
            .catch((err) => {
              ctx.logger('giftcard').warn(`无法获取群 ${session.guildId} 成员 ${id} 的信息: ${err}`);
              return { user: { id: id, name: id, nick: id }, nick: id }; // 回退对象
            })
        );
        const memberDetails = await Promise.all(memberDetailsPromises);

        invitedListStr = memberDetails.map((memberInfo, index) => {
          const invite = successfulInvites[index];
          const name = memberInfo?.nick || memberInfo?.user?.name || memberInfo?.user?.nick || invite.invited_id; // 优先使用昵称
          return `- ${h.escape(name)} (ID: ${invite.invited_id}) 于 ${invite.timestamp.toLocaleString()}`;
        }).join('\n');

      } catch (nameError) {
        ctx.logger('giftcard').warn(`获取群 ${session.guildId} 邀请列表成员名称时出错: ${nameError}`);
        invitedListStr = successfulInvites.map(invite => `- 用户 ID: ${invite.invited_id} (于 ${invite.timestamp.toLocaleString()})`).join('\n'); // 回退到ID
      }

      return `您在本群 (${session.guildId}) 成功邀请并获得奖励的成员列表：\n${invitedListStr}`;

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      ctx.logger('giftcard').error(`为用户 ${session.userId} 在群 ${session.guildId} 获取邀请记录失败: ${errorMessage}`);
      return '查询邀请记录时发生错误，请重试或联系管理员。';
    }
  });
  //指令：添加可多次使用的礼品卡
  cmd.subcommand('.addre <code> <frequency>', '添加可多次使用的礼品卡')
    .action(async ({ session }, code, frequency) => {

      if (!session || !session.userId) return;
      if (!isAdmin(session)) return '抱歉，只有管理员才能添加礼品卡。';
      if (!code) {
        return `请提供要添加的礼品卡代码，以空格分隔。\n用法: ${config.commandname}.addre <卡码1> <可用次数>`;
      }

      const inputCardCodes = [...new Set(code.trim().split(/\s+/).filter(code => code.length > 0))];
      if (inputCardCodes.length === 0) {
        return '未提供有效的礼品卡代码。';
      }

      const now = new Date();
      const adminId = session.userId;
      const successfullyAddedCodes: string[] = [];
      const alreadyExistedCodes: string[] = [];
      let errorMessage = '';

      try {
        const existingCards = await ctx.database.get('giftcard_cards',
          { card_code: { $in: inputCardCodes } },
          ['card_code']
        );
        const existingCodeSet = new Set(existingCards.map(card => card.card_code));

        const codesToAttemptUpsert = inputCardCodes.filter(code => !existingCodeSet.has(code));
        inputCardCodes.forEach(code => {
          if (existingCodeSet.has(code)) {
            alreadyExistedCodes.push(code);
          }
        });

        if (codesToAttemptUpsert.length > 0) {
          const newCardEntriesForUpsert = codesToAttemptUpsert.map(code => ({
            card_code: code,
            assigned_to_user_id: null,
            assigned_timestamp: null,
            added_by_admin_id: adminId,
            add_timestamp: now,
            left: parseInt(frequency),
            isreusable: true
          }));

          await ctx.database.upsert('giftcard_cards', newCardEntriesForUpsert);
          successfullyAddedCodes.push(...codesToAttemptUpsert);
        }

      } catch (error) {
        errorMessage = error instanceof Error ? error.message : String(error);
        ctx.logger('giftcard').error(`Upsert 礼品卡失败: ${errorMessage}`);
        errorMessage = `数据库操作时发生错误: ${errorMessage.substring(0, 100)}`;
      }

      let report = `礼品卡处理完成：\n成功添加 ${successfullyAddedCodes.length} 个新卡。`;
      if (successfullyAddedCodes.length > 0) {
        report += `\n新卡码: ${successfullyAddedCodes.join(', ')}`;
      }

      if (alreadyExistedCodes.length > 0) {
        report += `\n\n已存在 ${alreadyExistedCodes.length} 个（已跳过）：\n`;
        report += alreadyExistedCodes.map(code => `- ${h.escape(code)}`).join('\n');
      }
      if (errorMessage) {
        report += `\n\n处理过程中发生错误：${h.escape(errorMessage)}`;
        const possiblyFailed = inputCardCodes.filter(c => !successfullyAddedCodes.includes(c) && !alreadyExistedCodes.includes(c));
        if (possiblyFailed.length > 0) {
          report += `\n以下卡可能未能处理：${possiblyFailed.join(', ')}`;
        }
      }
      return report;
    })
}