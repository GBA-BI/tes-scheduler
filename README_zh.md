# veTES-scheduler

veTES-scheduler 组件主要用于调度 task 到集群中。包含以下功能：

## cache

负责 task/cluster/extra_priority/quota 等资源的缓存。
- taskCache。周期性轮询所有未结束的 task 并缓存。
  注意，只有在缓存中不存在时才会采用 `view=BASIC` 参数查询，得到 `Resources`、`BioosInfo` 等信息，
  否则只会采用 `view=MINIMAL` 参数查询，只同步 `State` 信息。
  另外，当其他模块需要 UpdateTask 时，会直接修改 cache，不用等到下个周期同步。
- clusterCache。周期性轮询所有 cluster 并缓存。
- extraPriorityCache。周期性轮询所有 extra_priority 并缓存。
- quotaCache。由于 quota 没有 list 接口（和公有云 quota 服务保持一致，便于适配），
  所以采用过期缓存的方式，每次查询时对查询结果进行缓存。
  过期时间与上述轮询缓存的周期相同。

## plugins

用于 scheduler，包括 sort、globalFilter、filter、score 等类型。
- sort。对于待调度的 task 进行排序。目前为 PrioritySort 插件。
- globalFilter。判断该 task 是否满足全局要求。目前为 ResourceQuota 插件。
- filter。判断该 task 是否满足对应 cluster 要求。目前包括 ClusterCapacity 和 ClusterLimit 插件。
- score。对于可行的 cluster 进行打分，区间为 \[0, 100\]。目前为 ClusterCapacity 插件。

## scheduler

轮询 cache 所有未调度的 task，`CANCELING` 的直接置为 `CANCELED`，`QUEUED` 的进入调度逻辑，执行各 plugin 逻辑。
- 只允许调度到健康状态的 cluster（`clusterNotReadyTimeout`）。
- sort plugin 只能有一个。
- globalFilter 和 filter 均必须全部插件通过才算通过。
- 所有 score 插件的结果需要进行平均，得到每个 cluster 的平均分。选择最高分进行调度
  若同时存在多个 cluster 得分相同，则随机选择其中一个。

## controller

负责一些调度相关的控制逻辑。
- rescheduleTasks。对于未结束的 task，若其被调度到的 cluster 已经不存在或者长时间不活跃（`clusterRescheduleTimeout`） 则将其重调度。
  `CANCELING` 的直接置为 `CANCELD`，其他则置为 `QUEUED` 并置空 `cluster_id`。
- markTasksFailedNotMeetLimits。对于未调度的 task，若其资源配置不满足任何一个 cluster 的 limits 要求，则直接置为 `SYSTEM_ERROR` 状态。
  该功能为了与原私部版逻辑保持一致，公有云版在 veTES-api 中的 normalize 逻辑已经限制。
  特殊处理，当不存在 cluster 时，不直接失败。

# FAQ

Q：为什么需要 controller？

A：因为有些逻辑并不单纯是调度逻辑（为一个 task 选择 cluster）。这些逻辑只能独立出去。

Q：为什么 ClusterLimit 要在 controller 和 scheduler 中实现两遍？

A：在 controller 中是为了直接将 task 置为失败，这个不是调度逻辑，不能放到 scheduler 中。
在 scheduler 中也需要再实现一遍，因为当存在部分 cluster 的 limits 满足部分不满足时，需要将不满足的过滤。

Q：为什么不把未调度的 `CANCELING` task 置为 `CANCELED` 的逻辑放到 controller？这个也不是调度逻辑。

A：为了避免 controller 和 scheduler 逻辑冲突。由于 cache、scheduler、controller 是三个不同的轮询，
可能存在 scheduler 将 task 调度后，controller 又将 task 直接置为 `CANCELD`。
